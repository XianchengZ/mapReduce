package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func readFromFile(fileName string) []byte {
	fileDescriptor, error := os.Open(fileName)
	if error != nil {
		log.Fatalf("Cannot read %v\n", fileName)
	}
	contents, error := io.ReadAll(fileDescriptor)
	if error != nil {
		log.Fatalf("Cannot read %v\n", fileName)
	}
	fileDescriptor.Close()
	return contents
}

func partition(intermediateKVP []KeyValue, nReduce int) [][]KeyValue {
	partitionedKVP := make([][]KeyValue, nReduce)

	for _, kvp := range intermediateKVP {
		bucketNumber := ihash(kvp.Key) % nReduce
		partitionedKVP[bucketNumber] = append(partitionedKVP[bucketNumber], kvp)
	}
	return partitionedKVP
}

func writeToIntermediateFiles(partitionedKVP [][]KeyValue, taskID int) {
	for partitionNumber, kvpArray := range partitionedKVP {

		tempFileName := fmt.Sprintf("mr-%v-%v-", taskID, partitionNumber)
		temp, err := os.CreateTemp("./", tempFileName)
		if err != nil {
			log.Fatalf("Cannot create file %v\n", tempFileName)
		}
		// write the kv pairs to the temp file
		enc := json.NewEncoder(temp)
		for _, kv := range kvpArray {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalln("JSON cannot be encoded")
			}
		}

		intermediateFileName := fmt.Sprintf("mr-%v-%v", taskID, partitionNumber)
		os.Rename(temp.Name(), intermediateFileName)
	}
}
