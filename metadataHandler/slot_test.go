package metadataHandler

import (
    "testing"
    "encoding/gob"
    "bytes"
)

func TestSerializationAndDeserialization(t *testing.T) {
    usecases := []Slot{
        Slot{0, 50, 1},
        Slot{50, 100, 2},
        Slot{150, 100, 3},
        Slot{500, 45, 21},
        Slot{50000000, 150, 500000},
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
        if decodedSlot.dataStoreSlot != usecase.dataStoreSlot {
            t.Errorf("Start byte offsets do not mach for usecase %v", i)
        }
        if decodedSlot.sizeOfData != usecase.sizeOfData {
            t.Errorf("Size of data do not match for usecase %v", i)
        }
        if decodedSlot.slotNo != usecase.slotNo {
            t.Errorf("Slot numbers do not match for usecase %v", i)
        }
    }
}
