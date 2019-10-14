package metadataHandler

import (
    "testing"
    "os"
    //"fmt"
    //"time"
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
            t.Errorf("Error %v when closing metadata", err)
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
    usecases := []uint32{2, 4, 6, 8, 10}
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

func TestWriteMetadataStatusByteBeforeWrite(t *testing.T) {
    slotNoUsecases := []uint32{2, 4, 6, 8, 10}
    createTestFolder(t)
    defer removeTestFolder(t)
    for _, v := range slotNoUsecases {
        metadata, err := GetMetadata("test/", 2500)
        if err != nil {
            t.Errorf("Error %v when trying to create metadata", err)
        }
        //Fill it with 0x00
        for i:=0; i<2500; i++ {
            metadata.data[i] = 0x00
        }
        //Set status byte
        res, err := metadata.checkAndSetStatusByteBeforeWrite(v, true)
        if err != nil {
            t.Errorf("Error %v when setting status byte", err)
        }
        //If result is not true, error
        if !res {
            t.Errorf("Got false when trying to set status byte")
        }
        //Check status byte
        if metadata.data[v * metdataIntervalLength] != SLOT_IN_USE | SLOT_BEING_MODIFIED {
            t.Errorf("Expected status byte: %v, got: %v", SLOT_IN_USE|SLOT_BEING_MODIFIED,
            metadata.data[v * metdataIntervalLength])
        }
    }
}

func TestWriteAttemptToInUseSLotReturnsError (t *testing.T) {
    slotNoUsecases := []uint32{2, 4, 6, 8, 10}
    createTestFolder(t)
    defer removeTestFolder(t)
    for _, v := range slotNoUsecases {
        metadata, err := GetMetadata("test/", 2500)
        if err != nil {
            t.Errorf("Error %v when trying to create metadata", err)
        }
        //Fill concerned byte with IN_USE | BEING_MODIFIES
        metadata.data[v * metdataIntervalLength] = SLOT_IN_USE | SLOT_BEING_MODIFIED
        //Try to set status byte
        res, err := metadata.checkAndSetStatusByteBeforeWrite(v, true)
        //If true is returned, test failed
        if res {
            t.Errorf("Resturned true when trying to set an already being modified slot")
        }
        //If no error is returned, test is failed
        if err == nil {
            t.Errorf("No error is returned when trying to set status byte")
        }
        //If error is returned, it should be of type MetadataSlotInUse
        if _, ok := err.(MetadataSlotInUse); !ok {
            t.Errorf("Error %v returned when trying to set status byte", err)
        }
    }
}

func TestWriteMetdataStatusByteAfterWrite(t *testing.T) {
    slotNoUsecases := []uint32{2, 4, 6, 8, 10}
    createTestFolder(t)
    defer removeTestFolder(t)
    for _, v := range slotNoUsecases {
        metadata, err := GetMetadata("test/", 2500)
        if err != nil {
            t.Errorf("Error %v when trying to create metadata", err)
        }
        //Fill concerned byte with IN_USE | BEING_MODIFIED
        status_byte := SLOT_IN_USE | SLOT_BEING_MODIFIED
        metadata.data[v * metdataIntervalLength] = status_byte
        //Try to reset the status
        metadata.checkAndUnsetStatusByteAfterWrite(v)
        //Check if it has been modified
        new_status_byte := status_byte & ^SLOT_BEING_MODIFIED
        if metadata.data[v * metdataIntervalLength] != new_status_byte {
            t.Errorf("The status byte %v doesnt match expected value of %v", metadata.data[v * metdataIntervalLength],
            new_status_byte)
        }
    }
}

func TestWriteSlotReturnsAppropriateErrorWhenSlotInUse(t *testing.T) {
    slotNoUsecases := []uint32{2, 4, 6, 8, 10}
    slot := Slot{50000000, 150, 500000}
    createTestFolder(t)
    defer removeTestFolder(t)
    for _, v := range slotNoUsecases {
        metadata, err := GetMetadata("test/", 2500)
        if err != nil {
            t.Errorf("Error %v when trying to create metadata", err)
        }
        //Fill the concerned byte with IN_USE | BEING_MODIFIED
        status_byte := SLOT_IN_USE | SLOT_BEING_MODIFIED
        metadata.data[v * metdataIntervalLength] = status_byte
        //Try to write a dummy slot
        err = metadata.WriteSlot(slot, v)
        //If no error, test failed
        if err == nil {
            t.Errorf("No error returned when trying to write to an occupied slot")
        }
        //Error should be of type MetadataSlotInUse
        if _, ok := err.(MetadataSlotInUse); !ok {
            t.Errorf("Error %v returned when MetadataSlotInUse expected", err)
        }
    }
}

//The following 2 functions are non deterministic. It could succeed or fail. But if any of the testcases succeeeds,
//It is a success
/*
func keepChangingSlotValues(keep_going *bool, byte_offset int) {
    //Create a new metadata
    metadata, _ := GetMetadata("test/", 2500)
   // fmt.Printf("Changing byte offset: %v\n", byte_offset)
    defer metadata.CloseFile()
    for ; *keep_going; {
        metadata.data[byte_offset] = metadata.data[byte_offset] << 1 | metadata.data[byte_offset] >> 7
        //fmt.Printf("Byte: %v\n", metadata.data[byte_offset])
    }
}

func TestWriteSlotReturnsMetadataWriteRetryErrorWhenRetriesExhausted(t *testing.T) {
    slotNoUsecases := []uint32{2, 4, 6, 8, 10}
    var keep_going_status bool
    slot := Slot{50000000, 150, 500000}
    createTestFolder(t)
    defer removeTestFolder(t)
    //slot_no := 4
    for _, slot_no := range slotNoUsecases {
        keep_going_status = true
        //Create a metadata
        metadata, err := GetMetadata("test/", 2500)
        if err != nil {
            t.Errorf("Error %v when trying to create metadata", err)
        }
        //Fill the concerned byte with IN_USE
        status_byte := SLOT_IN_USE
        metadata.data[slot_no * metdataIntervalLength] = status_byte
        //Next one as well
        metadata.data[slot_no * metdataIntervalLength + 1] = 0x86
        //Start a go routine to continually shift bits
        go keepChangingSlotValues(&keep_going_status, int(slot_no)*metdataIntervalLength+1)
        time.Sleep(10 * time.Millisecond)
        err = metadata.WriteSlot(slot, uint32(slot_no))
        //Make goroutine stop
        keep_going_status = false
        //If no error, test failed
        if err == nil {
            t.Errorf("No error returned")
        }
        //If error is not MetadataWriteRetryError, test failed
        if _, ok := err.(MetadataWriteRetryError); !ok {
            t.Errorf("Got error %v, when expected error is MetadataWriteRetryError", err)
        }
    }
} */

func TestWritesAvalibaleAcrossFileDescriptors(t *testing.T) {
    usecases := []uint32{2, 4, 6, 8, 10}
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
