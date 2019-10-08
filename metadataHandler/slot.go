package metadataHandler

import (
    "bytes"
    "encoding/gob"
)
//Each slot in the metadata file
//The max sizeOfData of each struct is 64*2 + 1*3 = 131 bytes
//Adding a buffer of 19bytes to describe variables and stuff, we can assume that each Slot
//type will not be more than 150 bytes. So each slot will be in an offset that is a multiple of 150 bytes
type Slot struct {
    dataStoreSlot       uint64
    sizeOfData          uint64
    slotNo              uint64
}

//Gob Encoder for Slot
func (s Slot) GobEncode() ([]byte, error) {
    var b bytes.Buffer
    encoder := gob.NewEncoder(&b)
    //Encode each of the members one by one
    err := encoder.Encode(s.dataStoreSlot)
    if err != nil {
        return nil, err
    }
    err = encoder.Encode(s.sizeOfData)
    if err != nil {
        return nil, err
    }
    err = encoder.Encode(s.slotNo)
    if err != nil {
        return nil , err
    }
    return b.Bytes(), nil
}

//Gob Decoder for Slot
func (s *Slot) GobDecode(b []byte) error {
    r := bytes.NewBuffer(b)
    decoder := gob.NewDecoder(r)
    //Decode each of the members in Slot
    err := decoder.Decode(&s.dataStoreSlot)
    if err != nil {
        return err
    }
    err = decoder.Decode(&s.sizeOfData)
    if err != nil {
        return err
    }
    err = decoder.Decode(&s.slotNo)
    if err != nil {
        return err
    }
    return nil
}
