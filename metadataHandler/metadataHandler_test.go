package metadataHandler

import (
    "testing"
    "os"
    "math"
    "bytes"
)

func createTestFolder(t *testing.T) {
    err := os.Mkdir("test", 0777)
    if err != nil {
        t.Errorf("Could not create a test directory. %v", err)
    }
}

func removeTestFolder(t *testing.T) {
    os.RemoveAll("test")
}

func TestCreateFileOfGivenSize(t *testing.T) {
    testSizes := []int64{75, 4097, 8193, 20000}
    expectedSizes := []int64{4096, 8192, 12288, 20480}
    //Create a directory and defer removing it
    /*
    err := os.Mkdir("test", 0777)
    if err != nil {
        t.Errorf("Could not create a test directory. %v", err)
    } */
    createTestFolder(t)
    defer removeTestFolder(t)
    for i, v := range testSizes {
        metadata, err := GetMetadata("test/", v)
        if err != nil {
            t.Errorf("Error %v when creating metadata", err)
        }
        err = metadata.CloseFile()
        if err != nil {
            t.Errorf("Error %v when closing meatadata", err)
        }
        f, err := os.Open("test/" + metadataFileName)
        if err != nil {
            t.Errorf("Error %v when opening metadata file test", err)
        }
        fi, err := f.Stat()
        if err != nil {
            t.Errorf("Error %v when getting stats for metadata", err)
        }
        if metadataSize := fi.Size(); metadataSize != expectedSizes[i] {
            t.Errorf("Sizes  of metadata file do not match. Expected %v but got %v", expectedSizes[i], metadataSize)
        }
    }
    //os.RemoveAll("test")
}

func TestWriteMetadataToFile(t *testing.T) {
    usecases := []uint64{2, 4, 6, 8, 10}
    slot := Slot{50000000, 150, 500000}
    /*
    err := os.Mkdir("test", 0777)
    if err != nil {
        t.Errorf("Could not create a test directory. %v", err)
    }*/
    createTestFolder(t)
    defer removeTestFolder(t)
    for _,v := range usecases {
        metadata, err := GetMetadata("test/", 2500)
        if err != nil {
            t.Errorf("Error %v when creating metadata", err)
        }
        err = metadata.WriteSlot(slot, v)
        if err != nil {
            t.Errorf("Error %v when writing a slot to metadata", err)
        }
        //Read the slot and compare the fields
        retrievedSlot, err := metadata.GetSlot(v)
        if err != nil {
            t.Errorf("Error %v when trying to read the slot", err)
        }
        if slot.dataStoreSlot != retrievedSlot.dataStoreSlot {
            t.Errorf("The retrieved slot fields do not match")
        }
        if slot.sizeOfData != retrievedSlot.sizeOfData {
            t.Errorf("Size of data does not match")
        }
    }
    //os.RemoveAll("test")
}

func TestWritesAvalibaleAcrossFileDescriptors(t *testing.T) {
    usecases := []uint64{2, 4, 6, 8, 10}
    slot := Slot{50000000, 150, 500000}
    /*
    err := os.Mkdir("test", 0777)
    if err != nil {
        t.Errorf("Could not create a test directory. %v", err)
    }*/
    createTestFolder(t)
    defer removeTestFolder(t)
    for _,v := range usecases {
        metadata, err := GetMetadata("test/", 2500)
        if err != nil {
            t.Errorf("Error %v when creating metadata", err)
        }
        err = metadata.WriteSlot(slot, v)
        if err != nil {
            t.Errorf("Error %v when writing a slot to metadata", err)
        }
        //Close metadata
        err = metadata.CloseFile()
        if err != nil {
            t.Errorf("Error %v when closing metadata", err)
        }
        //Get metadata again
        metadata, err = GetMetadata("test/", 2000)
        if err != nil {
            t.Errorf("Error %v when opening metadata again", err)
        }
        //Read the slot and compare the fields
        retrievedSlot, err := metadata.GetSlot(v)
        if err != nil {
            t.Errorf("Error %v when trying to read the slot", err)
        }
        if slot.dataStoreSlot != retrievedSlot.dataStoreSlot {
            t.Errorf("The retrieved slot fields do not match")
        }
        if slot.sizeOfData != retrievedSlot.sizeOfData {
            t.Errorf("Size of data does not match")
        }
    }
    //os.RemoveAll("test")
}

func TestCreationOfMetadataLookupFile(t *testing.T) {
    //Create some test cases
    testCases := []struct {
        inputSize       int64
        expectedSize    int64
    }{
        {
            inputSize:      4096,
            expectedSize:   int64(math.Ceil(float64(4096)/float64(metdataIntervalLength))),
        },
        {
            inputSize:      8192,
            expectedSize:   int64(math.Ceil(float64(8192)/float64(metdataIntervalLength))),
        },
        {
            inputSize:      40960,
            expectedSize:   int64(math.Ceil(float64(40960)/float64(metdataIntervalLength))),
        },
        {
            inputSize:      15000,
            //Expected size will be the same as before as lookup file wont shrink
            expectedSize:   int64(math.Ceil(float64(40960)/float64(metdataIntervalLength))),
        },
    }
    //Create test directory and defer removing it
    createTestFolder(t)
    defer removeTestFolder(t)
    for _, v := range testCases {
        testMetadata := Metadata {}
        err := testMetadata.createMetadataLookupFile("test/", v.inputSize)
        if err != nil {
            t.Errorf("Error %v when creating metadata lookup file", err)
        }
        //Sync and close the file
        testMetadata.lookup_file.Sync()
        testMetadata.lookup_file.Close()
        //Open the file
        f, err := os.OpenFile("test/" + metadataLookupFileName, os.O_RDWR|os.O_APPEND, 0777)
        if err != nil {
            t.Errorf("Error %v when trying to open metadata lookup file", err)
        }
        //Get the stats
        info, err := f.Stat()
        if err != nil {
            t.Errorf("Error %v when trying to get metadata lookup file stats", err)
        }
        //Compare the sizes
        if temp_s := info.Size(); temp_s != v.expectedSize {
            t.Errorf("The size of metadata lookup file is %v, which is not the expected value of %v",
            temp_s, v.expectedSize)
        }
    }
}

func TestMetadataFileInitialization(t *testing.T) {
        //Create testfolder and defer its removal
        createTestFolder(t)
        defer removeTestFolder(t)
        //testCases
        testCases := []struct {
            inputSize       int64
            padding_start   int64
            final_size      int64
        } {
            {
                inputSize:      4096,
                padding_start:  0,
                final_size:     int64(math.Ceil(float64(4096)/float64(metdataIntervalLength))),
            },
            {
                inputSize:      10000,
                //Previous size of the file
                padding_start:  int64(math.Ceil(float64(4096)/float64(metdataIntervalLength))),
                final_size:     int64(math.Ceil(float64(10000)/float64(metdataIntervalLength))),
            },
            {
                inputSize:      20000,
                //Previous size of the file
                padding_start:  int64(math.Ceil(float64(10000)/float64(metdataIntervalLength))),
                final_size:     int64(math.Ceil(float64(20000)/float64(metdataIntervalLength))),
            },
        }
        for _, v := range testCases {
            testMetadata := Metadata {}
            err := testMetadata.createMetadataLookupFile("test/", v.inputSize)
            if err != nil {
                t.Errorf("Error %v when creating metadata lookup file", err)
            }
            //Sync and close the file
            testMetadata.lookup_file.Sync()
            testMetadata.lookup_file.Close()
            //Open the file
            f, err := os.OpenFile("test/" + metadataLookupFileName, os.O_RDWR|os.O_APPEND, 0777)
            if err != nil {
                t.Errorf("Error %v when trying to open metadata lookup file", err)
            }
            file_data := make([]byte, v.final_size - v.padding_start)
            _, err = f.ReadAt(file_data, v.padding_start)
            if err != nil {
                t.Errorf("Errpr %v when reading from metadata lookup file", err)
            }
            for _, byte_value := range file_data {
                if byte_value != byte(0x00) {
                    t.Errorf("Metadata lookup not properly initialized")
                }
            }
        }
}

func TestIfMetadataLookupPreservesPreviousData(t *testing.T) {
    //Create a test folder and defer its removal
    createTestFolder(t)
    defer removeTestFolder(t)
    //Create metadata lookup file there
    f, err := os.OpenFile("test/" + metadataLookupFileName, os.O_RDWR|os.O_CREATE, 0777)
    if err != nil {
        t.Errorf("Error %v when creating a dummy metadata loookup file", err)
    }
    //Truncate it it 50 bytes
    err = f.Truncate(50)
    if err != nil {
        t.Errorf("Error %v when creating truncating dummy metadata lookup file", err)
    }
    //Fill it up with dummy values
    dummy_values := make([]byte, 50)
    for i:=0; i<50; i++ {
        dummy_values[i] = byte(i)
    }
    _, err = f.Write(dummy_values)
    if err != nil {
        t.Errorf("Error %v in writing data to dummy metadata lookup file", err)
    }
    //Sync and close it
    err = f.Sync()
    err = f.Close()
    if err != nil {
        t.Errorf("Error %v when closing dummy metadata lookup file", err)
    }
    //Create metadata obj
    testMetadata := Metadata {}
    //This adds another 50 bytes to the file
    testMetadata.createMetadataLookupFile("test/", 20000)
    //Close it
    err = testMetadata.lookup_file.Sync()
    err = testMetadata.lookup_file.Close()
    if err != nil {
        t.Errorf("Error %v when syncing and closing test metadata lookup", err)
    }
    //Open it again
    f, err = os.OpenFile("test/" + metadataLookupFileName, os.O_RDWR, 0777)
    if err != nil {
        t.Errorf("Error %v when creating a dummy metadata loookup file", err)
    }
    //Read the first 50 bytes
    new_values := make([]byte, 50)
    _, err = f.ReadAt(new_values, 0)
    if err != nil {
        t.Errorf("Error %v when reading a dummy metadata loookup file", err)
    }
    //Compare the values
    if res := bytes.Compare(dummy_values, new_values); res != 0 {
        t.Errorf("The existing data in metadata lookup changed after intialization")
    }
}
