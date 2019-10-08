package metadataHandler

import (
    "testing"
    "os"
)

func TestCreateFileOfGivenSize(t *testing.T) {
    testSizes := []int64{75, 100, 200, 300}
    for _, v := range testSizes {
        metadata, err := GetMetadata("test.ds", v)
        if err != nil {
            t.Errorf("Error %v when creating metadata", err)
        }
        err = metadata.CloseFile()
        if err != nil {
            t.Errorf("Error %v when closing meatadata", err)
        }
        f, err := os.Open("test.ds")
        if err != nil {
            t.Errorf("Error %v when opening metadata file test", err)
        }
        fi, err := f.Stat()
        if err != nil {
            t.Errorf("Error %v when getting stats for metadata", err)
        }
        if fi.Size() != v {
            t.Errorf("Sizes do not match!")
        }
    }
    os.Remove("test.ds")
}

func TestWriteMetadataToFile(t *testing.T) {
    usecases := []uint64{2, 4, 6, 8, 10}
    slot := Slot{50000000, 150, 500000}
    for _,v := range usecases {
        metadata, err := GetMetadata("test.ds", 2000)
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
    os.Remove("test.ds")
}

func TestWritesAvalibaleAcrossFileDescriptors(t *testing.T) {
    usecases := []uint64{2, 4, 6, 8, 10}
    slot := Slot{50000000, 150, 500000}
    for _,v := range usecases {
        metadata, err := GetMetadata("test.ds", 2000)
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
        metadata, err = GetMetadata("test.ds", 2000)
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
    os.Remove("test.ds")
}
