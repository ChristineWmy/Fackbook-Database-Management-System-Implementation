#include "Join.hpp"
#include <functional>

/*
 * TODO: Student implementation
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (MEM_SIZE_IN_PAGE - 1) after partition
 */
vector<Bucket> partition(
    Disk* disk, 
    Mem* mem, 
    pair<unsigned int, unsigned int> left_rel, 
    pair<unsigned int, unsigned int> right_rel) 
{
    mem -> reset();

    // Initialize a empty bucket
    Bucket initialBucket = Bucket(disk);
    vector<Bucket> partitions((MEM_SIZE_IN_PAGE - 1), initialBucket);

    // left relation
    for (unsigned int i = left_rel.first; i < left_rel.second; i++)
    {
        mem -> loadFromDisk(disk, i, MEM_SIZE_IN_PAGE - 1);
        Page* inputLeftBuffer = mem -> mem_page(MEM_SIZE_IN_PAGE - 1);
        for (unsigned int j = 0; j < inputLeftBuffer -> size(); j++)
        {
            Record record = inputLeftBuffer -> get_record(j);
            unsigned int recordHashValue = (record.partition_hash()) % (MEM_SIZE_IN_PAGE - 1);

            // Whether the current page is full
            if (mem -> mem_page(recordHashValue) -> full())
            {
                unsigned int leftPage = mem -> flushToDisk(disk, recordHashValue);
                partitions[recordHashValue].add_left_rel_page(leftPage);
            }
            (mem -> mem_page(recordHashValue)) -> loadRecord(record);
        }
    }

    for (unsigned int i = 0; i < MEM_SIZE_IN_PAGE - 1; i++)
    {
        if ((mem -> mem_page(i))-> size() > 0)
        {
            Record record = (mem -> mem_page(i)) -> get_record(0);
            unsigned int recordHashValue = (record.partition_hash()) % (MEM_SIZE_IN_PAGE - 1);
            partitions[recordHashValue].add_left_rel_page(mem -> flushToDisk(disk, i));
        }
    }

    mem ->reset();
    
    // right relation
    for (unsigned int i = right_rel.first; i < right_rel.second; i++)
    {
        mem -> loadFromDisk(disk, i, (MEM_SIZE_IN_PAGE - 1));
        Page* inputRightPage = mem -> mem_page(MEM_SIZE_IN_PAGE - 1);
        for (unsigned int j = 0; j < inputRightPage -> size(); j++)
        {
            Record record = inputRightPage -> get_record(j);
            unsigned int recordHashValue = (record.partition_hash()) % (MEM_SIZE_IN_PAGE - 1);
           
            if (mem -> mem_page(recordHashValue) -> full())
            {
                partitions[recordHashValue].add_right_rel_page(mem -> flushToDisk(disk, recordHashValue));
            }
            (mem -> mem_page(recordHashValue)) -> loadRecord(record);
        }
    }

    for (unsigned int i = 0; i < MEM_SIZE_IN_PAGE - 1; i++)
    {
        if (mem -> mem_page(i) -> size() > 0)
        {
            Record record = (mem -> mem_page(i)) -> get_record(0);
            unsigned int recordHashValue = (record.partition_hash()) % (MEM_SIZE_IN_PAGE - 1);
            partitions[recordHashValue].add_right_rel_page(mem -> flushToDisk(disk, i));
        }
    }

    return partitions;
}

/*
 * TODO: Student implementation
 * Input: Disk, Memory, Vector of Buckets after partition
 * Output: Vector of disk page ids for join result
 */
vector<unsigned int> probe(Disk* disk, Mem* mem, vector<Bucket>& partitions) 
{
    mem -> reset();
    vector<unsigned int> results;

    unsigned int leftNum = 0;
    unsigned int rightNum = 0;

    for (unsigned int i = 0; i < partitions.size(); i++)
    {
        leftNum += partitions[i].num_left_rel_record;
        rightNum += partitions[i].num_right_rel_record;
    }

    string order = "";
    if (rightNum >= leftNum)
    {
        order = "left";
    }
    else
    {
        order = "right";
    }

    Page* output = mem -> mem_page(MEM_SIZE_IN_PAGE - 1);
    for (unsigned int i = 0; i < partitions.size(); i++)
    {
        if (order == "left")
        {
            vector<unsigned int> leftPageIdInDisk = partitions[i].get_left_rel();
            for (unsigned int j = 0; j < leftPageIdInDisk.size(); j++)
            {
                mem -> loadFromDisk(disk, leftPageIdInDisk[j], (MEM_SIZE_IN_PAGE - 2));
                Page* inputLeftPage = mem -> mem_page(MEM_SIZE_IN_PAGE - 2);
                for (unsigned int k = 0; k < inputLeftPage -> size(); k++)
                {
                    Record record = inputLeftPage -> get_record(k);
                    unsigned int leftHashValue = (record.probe_hash()) % (MEM_SIZE_IN_PAGE - 2);
                    (mem -> mem_page(leftHashValue)) -> loadRecord(record);
                }
            }
            vector<unsigned int> rightPageIdInDisk = partitions[i].get_right_rel();
            for (unsigned int j = 0; j < rightPageIdInDisk.size(); j++)
            {
                mem -> loadFromDisk(disk, rightPageIdInDisk[j], (MEM_SIZE_IN_PAGE - 2));
                Page* inputRightPage = mem -> mem_page(MEM_SIZE_IN_PAGE - 2);
                for (unsigned int k = 0; k < inputRightPage -> size(); k++)
                {
                    Record recordRight = inputRightPage -> get_record(k);
                    unsigned int rightHashValue = (recordRight.probe_hash()) % (MEM_SIZE_IN_PAGE - 2);

                    Page* matchingPage = mem -> mem_page(rightHashValue);
                    for (unsigned int r = 0; r < matchingPage -> size(); r++)
                    {
                        Record recordLeft = matchingPage -> get_record(r);
                        if (recordRight == recordLeft)
                        {
                            if (output -> full())
                            {
                                results.push_back(mem -> flushToDisk(disk, (MEM_SIZE_IN_PAGE - 1)));
                            }
                            (mem -> mem_page(MEM_SIZE_IN_PAGE - 1)) -> loadPair(recordLeft, recordRight);
                            // output -> loadPair(recordLeft, recordRight);
                        }
                    }
                }
            }
            for (unsigned int j = 0; j < MEM_SIZE_IN_PAGE - 2; j++)
            {
                (mem -> mem_page(j)) -> reset();
            }
        }


        if (order == "right")
        {
            vector<unsigned int> rightPageIdInDisk = partitions[i].get_right_rel();
            for (unsigned int j = 0; j < rightPageIdInDisk.size(); j++)
            {
                mem -> loadFromDisk(disk, rightPageIdInDisk[j], MEM_SIZE_IN_PAGE - 2);
                Page* inputRightPage = mem -> mem_page(MEM_SIZE_IN_PAGE - 2);
                for (unsigned int k = 0; k < inputRightPage -> size(); k++)
                {
                    Record record = inputRightPage -> get_record(k);
                    unsigned int rightHashValue = (record.probe_hash()) % (MEM_SIZE_IN_PAGE - 2);
                    (mem -> mem_page(rightHashValue)) -> loadRecord(record);
                }
            }
            vector<unsigned int> leftPageIdInDisk = partitions[i].get_left_rel();
            for (unsigned int j = 0; j < leftPageIdInDisk.size(); j++)
            {
                mem -> loadFromDisk(disk, leftPageIdInDisk[j], (MEM_SIZE_IN_PAGE - 2));
                Page* inputLeftPage = mem -> mem_page(MEM_SIZE_IN_PAGE - 2);
                for (unsigned int k = 0; k < inputLeftPage -> size(); k++)
                {
                    Record recordLeft = inputLeftPage -> get_record(k);
                    unsigned int leftHashValue = (recordLeft.probe_hash()) % (MEM_SIZE_IN_PAGE - 2);
                    Page* matchingPage = mem -> mem_page(leftHashValue);
                    for (unsigned int r = 0; r < matchingPage -> size(); r++)
                    {
                        Record recordRight = matchingPage -> get_record(r);
                        if (recordLeft == recordRight)
                        {
                            if (output -> full())
                            {
                                results.push_back(mem -> flushToDisk(disk, (MEM_SIZE_IN_PAGE - 1)));
                            }
                            (mem -> mem_page(MEM_SIZE_IN_PAGE - 1)) -> loadPair(recordLeft, recordRight);
                            // output -> loadPair(recordLeft, recordRight);
                        }
                    }
                }
            }
            for (unsigned int j = 0; j < MEM_SIZE_IN_PAGE - 2; j++)
            {
                (mem -> mem_page(j)) -> reset();
            }
        }
    }
    if (output -> size() > 0)
    {
        results.push_back(mem -> flushToDisk(disk, MEM_SIZE_IN_PAGE - 1));
    }
    return results;
}

