#include <mpi.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>

using namespace std;

#define TRACKER_RANK 0
#define MAX_FILES 10
#define MAX_FILENAME 15
#define HASH_SIZE 32
#define MAX_CHUNKS 100

#define TRACKER_INIT 0
#define DOWNLOAD 1
#define UPLOAD 2
#define SEGMENT 3
#define OK 1
#define NOT_OK 0


struct File {
    vector<int> owner_rank;
    string name;
    int num_segments = 0;
    vector<string> segments;
};

struct Peer {
    int num_owned_files = 0;
    int num_wanted_files = 0;
    vector<File> owned_files;
    vector<File> wanted_files;
    vector<int> wanted_owners;
    int rank;
};

struct Tracker {
    int num_owned_files = 0;
    vector<File> owned_files;
};


void *download_thread_func(void *arg)
{
    Peer peer = *(Peer*) arg;

    // spune trackerului ce fisiere doreste
    int ok = 1;
    
    for (int j = 0; j < peer.num_wanted_files; j++) {
        
        MPI_Send(&ok, 1, MPI_INT, TRACKER_RANK, DOWNLOAD, MPI_COMM_WORLD);
        MPI_Send(peer.wanted_files[j].name.c_str(), MAX_FILENAME, MPI_CHAR, TRACKER_RANK, DOWNLOAD, MPI_COMM_WORLD);

        std::ofstream fout("client" + to_string(peer.rank) + "_" + peer.wanted_files[j].name);
        if (!fout.is_open())
        {
            cout << "Nu s-a putut deschide fisierul " + to_string(peer.rank) << endl;
            return NULL;
        }

        //primeste numarul de owneri ale fisierului respectiv
        int num_owners;
        MPI_Recv(&num_owners, 1, MPI_INT, TRACKER_RANK, DOWNLOAD, MPI_COMM_WORLD, MPI_STATUS_IGNORE);


        //primeste nr de hashuri
        int num_hashes;
        MPI_Recv(&num_hashes, 1, MPI_INT, TRACKER_RANK, DOWNLOAD, MPI_COMM_WORLD, MPI_STATUS_IGNORE);


        //primeste fiecare owner in parte si il retine
        for (int k = 0; k < num_owners; k++) {
            int owner_tmp;
            MPI_Recv(&owner_tmp, 1, MPI_INT, TRACKER_RANK, DOWNLOAD, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            peer.wanted_owners.push_back(owner_tmp);
            int curr_up_tmp;            
        }

        File f_new;
        f_new.name = peer.wanted_files[j].name;
        f_new.num_segments = 0;
        f_new.owner_rank.push_back(peer.rank);

        
        for (int i = 0; i < num_hashes; i++) {
            
            int update_tracker;
            if (i % 10 == 0) {
                update_tracker = 1;
            } else {
                update_tracker = 0;
            }
            MPI_Send(&update_tracker, 1, MPI_INT, TRACKER_RANK, UPLOAD, MPI_COMM_WORLD);

            //alege cel mai liber peer
            int owner = peer.wanted_owners[i%num_owners];
            
            int ok = 1;
            MPI_Send(&ok, 1, MPI_INT, owner, UPLOAD, MPI_COMM_WORLD);

            MPI_Send(peer.wanted_files[j].name.c_str(), MAX_FILENAME, MPI_CHAR, owner, UPLOAD, MPI_COMM_WORLD);
            MPI_Send(&i, 1, MPI_INT, owner, UPLOAD, MPI_COMM_WORLD);
            char hash[HASH_SIZE + 1];
            MPI_Recv(hash, HASH_SIZE + 1, MPI_CHAR, owner, SEGMENT, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            // string hash_str = hash + '\0';
            fout << hash << "\n";
            f_new.segments.push_back(hash);

        }

        peer.owned_files.push_back(f_new);
        fout.close();
        peer.wanted_owners.clear();
    }


    ok = 0;
    MPI_Send(&ok, 1, MPI_INT, TRACKER_RANK, DOWNLOAD, MPI_COMM_WORLD);

    return NULL;
}

void *upload_thread_func(void *arg)
{
    Peer peer = *(Peer*) arg;

    int ok = 1;

    MPI_Status status;

    while (ok) {
        MPI_Recv(&ok, 1, MPI_INT, MPI_ANY_SOURCE, UPLOAD, MPI_COMM_WORLD, &status);
        if (!ok) 
            break;

        char wanted_file_name[MAX_FILENAME];
        MPI_Recv(wanted_file_name, MAX_FILENAME, MPI_CHAR, status.MPI_SOURCE, UPLOAD, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        int hash_id;
        MPI_Recv(&hash_id, 1, MPI_INT, status.MPI_SOURCE, UPLOAD, MPI_COMM_WORLD, MPI_STATUS_IGNORE);


        auto it = find_if(peer.owned_files.begin(), peer.owned_files.end(), [&wanted_file_name](const File &el) {return el.name == wanted_file_name;});
        File f = *it;

        MPI_Send(f.segments[hash_id].c_str(), HASH_SIZE + 1, MPI_CHAR, status.MPI_SOURCE, SEGMENT, MPI_COMM_WORLD);


    }


    return NULL;
}

void tracker(int numtasks, int rank) {
    Tracker tracker;
    tracker.num_owned_files = 0;

    int num_files;
    for (int i = 1; i < numtasks; i++) {
        MPI_Recv(&num_files, 1, MPI_INT, i, TRACKER_INIT, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        for (int j = 0; j < num_files; j++) {
            File new_file;
            MPI_Recv(&new_file.num_segments, 1, MPI_INT, i, TRACKER_INIT, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            char name[MAX_FILENAME];
            MPI_Recv(name, MAX_FILENAME, MPI_CHAR, i, TRACKER_INIT, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            new_file.name = name;

            int owner_rank;
            MPI_Recv(&owner_rank, 1, MPI_INT, i, TRACKER_INIT, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            new_file.owner_rank.push_back(owner_rank);


            for (int k = 0; k < new_file.num_segments; k++) {
                char segment[HASH_SIZE];
                MPI_Recv(segment, HASH_SIZE, MPI_CHAR, i, TRACKER_INIT, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                new_file.segments.push_back(segment);
            }

            auto it = find_if(tracker.owned_files.begin(), tracker.owned_files.end(), [&new_file](const File &el) {return el.name == new_file.name;});
            
            if (it == tracker.owned_files.end()) {
                tracker.owned_files.push_back(new_file);
            } else {
                it->owner_rank.push_back(owner_rank);
            }
        }
        tracker.num_owned_files = tracker.owned_files.size();
    }

    int ok = 1;
    for (int i = 1; i < numtasks; i++) {
        MPI_Send(&ok, 1, MPI_INT, i, TRACKER_INIT, MPI_COMM_WORLD);

    }
    int finished = 0;

    //fisiere dorite de client

    while (finished < numtasks - 1) {
        MPI_Status status;


        MPI_Recv(&ok, 1, MPI_INT, MPI_ANY_SOURCE, DOWNLOAD, MPI_COMM_WORLD, &status);
        
        if (!ok) {
            finished++;
            
            if (finished >= numtasks - 1) {
                break;
            }
            continue;
        }



        // primeste numele fisierului dorit
        char wanted_file_name[MAX_FILENAME];
        MPI_Recv(wanted_file_name, MAX_FILENAME, MPI_CHAR, status.MPI_SOURCE, DOWNLOAD, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        // gaseste fisierul
        auto it = find_if(tracker.owned_files.begin(), tracker.owned_files.end(), [&wanted_file_name](const File &el) {return el.name == wanted_file_name;});
        File f = *it;
        
        //ii spune cati owneri are fisierul
        int num_owners = f.owner_rank.size();
        MPI_Send(&num_owners, 1, MPI_INT, status.MPI_SOURCE, DOWNLOAD, MPI_COMM_WORLD);

        //trimite nr de hashuri
        int num_hashes = f.num_segments;
        MPI_Send(&num_hashes, 1, MPI_INT, status.MPI_SOURCE, DOWNLOAD, MPI_COMM_WORLD);

        //trimite ownerii fiecare in parte
        for (int k = 0; k < f.owner_rank.size(); k++) {
            MPI_Send(&f.owner_rank[k], 1, MPI_INT, status.MPI_SOURCE, DOWNLOAD, MPI_COMM_WORLD);
        }

        int update_tracker;
        MPI_Recv(&update_tracker, 1, MPI_INT, status.MPI_SOURCE, UPLOAD, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        if (update_tracker) {
            if (find(f.owner_rank.begin(), f.owner_rank.end(), status.MPI_SOURCE) == f.owner_rank.end()) {
                f.owner_rank.push_back(status.MPI_SOURCE);
            }
        }
        

    }

    for (int i = 1; i < numtasks; i++) {
        int ok = 0;
        MPI_Send(&ok, 1, MPI_INT, i, UPLOAD, MPI_COMM_WORLD);
    }
  

}


void peer(int numtasks, int rank) {
    pthread_t download_thread;
    pthread_t upload_thread;
    void *status;
    int r;

    //citire din fisier
    std::ifstream fin("in" + to_string(rank) + ".txt");
    if (!fin.is_open())
    {
        cout << "Nu s-a putut deschide fisierul " + to_string(rank) << endl;
        return;
    }

    Peer peer;
    peer.rank = rank;
    fin >> peer.num_owned_files; 

    for (int i = 0; i < peer.num_owned_files; ++i)
    {
        File new_file;
        new_file.owner_rank.push_back(rank);
        fin >> new_file.name >> new_file.num_segments;

        for (int j = 0; j < new_file.num_segments; ++j)
        {
            string segment;
            fin >> segment;
            new_file.segments.push_back(segment);
        }
        peer.owned_files.push_back(new_file);
    }

    fin >> peer.num_wanted_files; 

    for (int i = 0; i < peer.num_wanted_files; ++i)
    {
        File new_file;
        new_file.owner_rank.push_back(-1);
        fin >> new_file.name;
        peer.wanted_files.push_back(new_file);
    }
    fin.close(); 


    MPI_Send(&peer.num_owned_files, 1, MPI_INT, TRACKER_RANK, TRACKER_INIT, MPI_COMM_WORLD);
    for (int i = 0; i < peer.num_owned_files; i++) {
        MPI_Send(&peer.owned_files[i].num_segments, 1, MPI_INT, TRACKER_RANK, TRACKER_INIT, MPI_COMM_WORLD);

        MPI_Send(peer.owned_files[i].name.c_str(), MAX_FILENAME, MPI_CHAR, TRACKER_RANK, TRACKER_INIT, MPI_COMM_WORLD);

        MPI_Send(&rank, 1, MPI_INT, TRACKER_RANK, TRACKER_INIT, MPI_COMM_WORLD);

        for (int k = 0; k < peer.owned_files[i].num_segments; k++) {
            MPI_Send(peer.owned_files[i].segments[k].c_str(), MAX_FILENAME, MPI_CHAR, TRACKER_RANK, TRACKER_INIT, MPI_COMM_WORLD);
        }
    }


    int tracker_ok;
    MPI_Recv(&tracker_ok, 1, MPI_INT, TRACKER_RANK, TRACKER_INIT, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    if (tracker_ok != 1) {
        cout << "not ok" << endl;
    }
    // cout << rank << endl;


   





    r = pthread_create(&download_thread, NULL, download_thread_func, (void *) &peer);
    if (r) {
        printf("Eroare la crearea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_create(&upload_thread, NULL, upload_thread_func, (void *) &peer);
    if (r) {
        printf("Eroare la crearea thread-ului de upload\n");
        exit(-1);
    }

    r = pthread_join(download_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_join(upload_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de upload\n");
        exit(-1);
    }
}
 
int main (int argc, char *argv[]) {
    int numtasks, rank;
 
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    if (provided < MPI_THREAD_MULTIPLE) {
        fprintf(stderr, "MPI nu are suport pentru multi-threading\n");
        exit(-1);
    }
    MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == TRACKER_RANK) {
        tracker(numtasks, rank);
    } else {
        peer(numtasks, rank);
    }

    MPI_Finalize();
}
