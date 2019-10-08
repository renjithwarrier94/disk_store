package metadataHandler

import (
    "testing"
    "encoding/gob"
    "bytes"
)

func TestSerializationAndDeserialization(t *testing.T) {
    usecases := []Slot{
        Slot{0, 50, false, false, false, 1},
        Slot{50, 100, false, false, false, 2},
        Slot{150, 100, true, false, true, 3},
        Slot{500, 45, false, true, true, 21},
        Slot{50000000, 150, true, true, true, 500000},
    }
    for i, usecase := range usecases {
        var buffer bytes.Buffer
        encoder := gob.NewEncoder(&buffer)
        err := encoder.Encode(usecase)
        //b, err := usecase.GobEncode()
        if err != nil {
            t.Errorf("Error when encoding usecase %v. Error: %v", i, err)
        }
        b := bytes.NewBuffer(buffer.Bytes())
        var decodedSlot Slot
        //err := decodedSlot.GobDecode(b)
        decoder := gob.NewDecoder(b)
        err = decoder.Decode(&decodedSlot)
        if err != nil {
            t.Errorf("Error when decoding usecase %v. Error: %v", i, err)
        }
        if decodedSlot.startByteOffset != usecase.startByteOffset {
            t.Errorf("Start byte offsets do not mach for usecase %v", i)
        }
        if decodedSlot.sizeOfData != usecase.sizeOfData {
            t.Errorf("Size of data do not match for usecase %v", i)
        }
        if decodedSlot.beingModified != usecase.beingModified {
            t.Errorf("Being modified do not match for usecase %v", i)
        }
        if decodedSlot.markedForDeletion != usecase.markedForDeletion {
            t.Errorf("Marked for deletion do not match for usecase %v", i)
        }
        if decodedSlot.isDeleted != usecase.isDeleted {
            t.Errorf("Is deleted do not match for usecase %v", i)
        }
        if decodedSlot.slotNo != usecase.slotNo {
            t.Errorf("Slot numbers do not match for usecase %v", i)
        }
    }
}
