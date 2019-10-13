package metadataHandler

import (
    "testing"
    "os"
    "math"
    "bytes"
)

func TestCreationOfMetadataLookupFile(t *testing.T) {
    //Create some test cases
    testCases := []struct {
        inputSize       int64
        num_slots       uint32
    }{
        {
            inputSize:      4096,
            num_slots:      uint32(math.Floor(float64( 4096 )/float64( metdataIntervalLength ))),
        },
        {
            inputSize:      8192,
            num_slots:      uint32(math.Floor(float64( 8192 )/float64( metdataIntervalLength ))),
        },
        {
            inputSize:      40960,
            num_slots:      uint32(math.Floor(float64( 40960 )/float64( metdataIntervalLength ))),
        },
        {
            inputSize:      15000,
            //Expected size will be the same as before as lookup file wont shrink
            num_slots:      uint32(math.Floor(float64( 40960 )/float64( metdataIntervalLength ))),
        },
    }
    //min_req_size = int64(math.Ceil( math.Ceil(float64(metadata.num_slots)/8.0) / 4.0 ) * 4)
    //Create test directory and defer removing it
    createTestFolder(t)
    defer removeTestFolder(t)
    for _, v := range testCases {
        testMetadata := Metadata {}
        testMetadata.num_slots = v.num_slots
        //Calculate expected size from number of slots
        expectedSize := int64(math.Ceil( math.Ceil(float64(v.num_slots)/8.0) / 4.0 ) * 4)
        err := testMetadata.createMetadataLookupFile("test/")
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
        if temp_s := info.Size(); temp_s != expectedSize {
            t.Errorf("The size of metadata lookup file is %v, which is not the expected value of %v",
            temp_s, expectedSize)
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
            initial_slots   uint32
            final_slots     uint32
        } {
            {
                inputSize:      4096,
                initial_slots:  0,
                final_slots:     uint32(math.Floor(float64( 4096 )/float64( metdataIntervalLength ))),
            },
            {
                inputSize:      10000,
                //Previous size of the file
                initial_slots:  uint32(math.Floor(float64( 4096 )/float64( metdataIntervalLength ))),
                final_slots:    uint32(math.Floor(float64( 10000 )/float64( metdataIntervalLength ))),
            },
            {
                inputSize:      20000,
                //Previous size of the file
                initial_slots:  uint32(math.Floor(float64( 10000 )/float64( metdataIntervalLength ))),
                final_slots:    uint32(math.Floor(float64( 20000 )/float64( metdataIntervalLength ))),
            },
        }
        for _, v := range testCases {
            testMetadata := Metadata {}
            testMetadata.num_slots = v.final_slots
            padding_start := int64(math.Ceil( math.Ceil(float64(v.initial_slots)/8.0) / 4.0 ) * 4)
            final_size := int64(math.Ceil( math.Ceil(float64(v.final_slots)/8.0) / 4.0 ) * 4)
            err := testMetadata.createMetadataLookupFile("test/")
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
            file_data := make([]byte, final_size - padding_start)
            _, err = f.ReadAt(file_data, padding_start)
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
    testMetadata.num_slots = uint32(math.Floor(float64( 20000 )/float64( metdataIntervalLength )))
    testMetadata.createMetadataLookupFile("test/")
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
