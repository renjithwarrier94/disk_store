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
            //inputSize:      4096,
            //num_slots:      uint32(math.Floor(float64( 4096 )/float64( metdataIntervalLength ))),
            //Min size is 50KB (51200 bytes)
            inputSize:      size_step - 3000,
            num_slots:      uint32(math.Floor(float64( size_step )/float64( metdataIntervalLength ))),
        },
        {
            //inputSize:      8192,
            //num_slots:      uint32(math.Floor(float64( 8192 )/float64( metdataIntervalLength ))),
            //The next step is 2*size_step
            inputSize:      size_step + 2000,
            num_slots:      uint32(math.Floor(float64( 2*size_step )/float64( metdataIntervalLength ))),
        },
        {
            //inputSize:      40960,
            //num_slots:      uint32(math.Floor(float64( 40960 )/float64( metdataIntervalLength ))),
            //Next is 3*size_step
            inputSize:      2*size_step + 500,
            num_slots:      uint32(math.Floor(float64( 3*size_step )/float64( metdataIntervalLength ))),
        },
        {
            //inputSize:      15000,
            //num_slots:      uint32(math.Floor(float64( 40960 )/float64( metdataIntervalLength ))),
            inputSize:      size_step - 2000,
            //Expected size will be the same as before as lookup file wont shrink
            num_slots:      uint32(math.Floor(float64( 3*size_step )/float64( metdataIntervalLength ))),
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

func markOpenSlotsInLookup(metadata *Metadata, openSlots []uint32) {
    //Calculate the byte position of each open slot
    byte_offsets := make(map[uint32][]uint32)
    for _, slot := range openSlots {
        byte_offset := uint32(math.Floor(float64(slot)/8.0))
        if _, ok := byte_offsets[byte_offset]; !ok {
            byte_offsets[byte_offset] = []uint32{}
        }
        byte_offsets[byte_offset] = append(byte_offsets[byte_offset], slot)
    }
    max_byte := uint32(math.Floor(float64(metadata.num_slots)/8.0))
    //Iterate though the lookup file and set all bits except those belonging to openSlots to 1 
    for i:=uint32(0); i<max_byte; i++ {
        //Set it to all 1s anyway
        metadata.lookup[i] = 0xff
       //Check if the byte offsets contains the byte
       if slots, ok := byte_offsets[i]; ok {
            //If it does,
           //Loop through all the slots applicable in this byte
           for _, j := range slots {
               //Get the bit offset
                bit_offset := j % 8
                //Get the appropritate bit occupied value
                required_bit := SLOT_OCCUPIED_VALUES[bit_offset]
                metadata.lookup[i] = metadata.lookup[i] & ^required_bit
            }
        }
    }
}

func listContains(test_list []uint32, test_num uint32) (bool, int) {
    for i, v:= range test_list {
        if v == test_num {
            return true, i
        }
    }
    return false, 0
}

func listRemove(test_list []uint32, index int) []uint32 {
    return append(test_list[:index], test_list[index+1:]...)
}

func testIfSlotIsReserved(metadata *Metadata, slot_no uint32) bool {
    byte_offset := uint32(math.Floor(float64(slot_no)/8.0))
    bit_offset := slot_no % 8
    return metadata.lookup[byte_offset] & SLOT_OCCUPIED_VALUES[bit_offset] > 0x00
}

func testIfSlotsAreFree(metadata *Metadata, free_slots []uint32) bool {
    for _, v := range free_slots {
        byte_offset := uint32(math.Floor(float64(v) / 8.0))
        bit_offset := v % 8
        if metadata.lookup[byte_offset] & SLOT_OCCUPIED_VALUES[bit_offset] != 0x00 {
            return false
        }
    }
    return true
}

func TestAllocationOfAFreeSlot(t *testing.T) {
    //Create test folder and defer deleting it
    createTestFolder(t)
    defer removeTestFolder(t)
    //Create a slice of open spaces
    openSlots := []uint32{2, 4, 8, 10, 12, 14}
    //Create the number of times to test it
    numTests := 4
    //create metadata
    metadata, err := GetMetadata("test/", 4096)
    if err != nil {
        t.Errorf("Error %v when trying to obtain metadata", err)
    }
    //Mark all the free slots
    markOpenSlotsInLookup(metadata, openSlots)
    //Repeat test for n times
    for i:=0; i<numTests; i++ {
        //Get an open slot
        slot_no, err := metadata.getOpenSlot()
        if err != nil {
            t.Errorf("Error %v when reserving a slot", err)
        }
        //Check if the slot was actually free
        res, loc := listContains(openSlots, slot_no)
        if !res {
            t.Errorf("%v returned as free slot when the only free slots available are %v. Try: %v",
            slot_no, openSlots, i)
        }
        openSlots = listRemove(openSlots, loc)
        //Check if the bit is set to occupied
        if !testIfSlotIsReserved(metadata, slot_no) {
            t.Errorf("The bit in lookup is not set after alloting slot")
        }
        if !testIfSlotsAreFree(metadata, openSlots) {
            t.Errorf("The remaining open slots are not showing open")
        }
    }
}

func TestOutOfSlotsErrorWhenNoSlotsAvailable(t *testing.T) {
    //Create test folder and defer deleting it
    createTestFolder(t)
    defer removeTestFolder(t)
    //Create an empty slice of open slots
    openSlots := []uint32{}
    //Create metadata
    metadata, err := GetMetadata("test/", 4096)
    if err != nil {
        t.Errorf("Error %v when trying to obtain metadata", err)
    }
    //Mark all the free (none) slots
    markOpenSlotsInLookup(metadata, openSlots)
    //Try to get an open slot
    _, err = metadata.getOpenSlot()
    //If err is not nil, test failed
    if err == nil {
        t.Errorf("No error returned when all the slots are used up")
    }
    _, ok := err.(metadataOutOfSlots)
    if !ok {
        t.Errorf("Error %v returned instead of metadataOutOfSlots", err)
    }
}

func getOpenSlotFromMetadataa(metadata *Metadata, ch chan uint32, t *testing.T) {
    slot_no, err := metadata.getOpenSlot()
    if err != nil {
        t.Errorf("Error %v when trying to reserve a slot", err)
    }
    ch <- slot_no
}

//TestAtomicAllocation - 
    //Multiple goroutines competing to get slots
func TestAtomicAllocationOfASlot(t *testing.T) {
    //Create test folder and defer its deletion
    createTestFolder(t)
    defer removeTestFolder(t)
    //Create a slice of open slots
    openSlots := []uint32{2, 4, 6, 8, 9, 10, 11, 12, 13, 14, 15}
    //Number of go routines to run
    num_go_routines := len(openSlots) 
    ch := make(chan uint32, num_go_routines)
    //Create metadata
    metadata, err := GetMetadata("test/", 4096)
    if err != nil {
        t.Errorf("Error %v when trying to create metadata", err)
    }
    //Mark all the free slots
    markOpenSlotsInLookup(metadata, openSlots)
    //Create goroutines to check 
    for i:=0; i<num_go_routines; i++ {
        go getOpenSlotFromMetadataa(metadata, ch, t)
    }
    //Get all the slot values and check if its uinique
    for i:=0; i<num_go_routines; i++ {
        slot_no := <-ch
        res, loc := listContains(openSlots, slot_no)
        if !res {
            t.Errorf("One of the go routines returned %v when the only slots available were %v",
        slot_no, openSlots)
        }
        openSlots = listRemove(openSlots, loc)
    }
}
