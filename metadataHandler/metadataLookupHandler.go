package metadataHandler

//All methods related to metadataLookup file

import (
    "os"
    "syscall"
    "github.com/pkg/errors"
    "math"
)

//The values for each filled slot in a byte
//const SLOT_OCCUPIED_VALUES [8]byte = [8]byte{0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0x01}

//Create/open a metadata lookup file
func (metadata *Metadata) createMetadataLookupFile(path string) error {
    var f *os.File
    //To fill up the lookup file with empty bytes, we need the old and new size. For new file, old_size is 0
    var old_size, min_req_size int64
    //Set the size to the min required size - each byte in lookup can represent 8 slots
    //Also since atomic operations require atleast 4 bytes, the size should be a multiple of 4 bytes
    min_req_size = int64(math.Ceil( math.Ceil(float64(metadata.num_slots)/8.0) / 4.0 ) * 4)
    //Open/create the file
    f, err := os.OpenFile(path + metadataLookupFileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0777)
    if err != nil {
        return errors.Wrap(err, "Could not create metadata lookup file")
    }
    //Get the size of the file
    info, err := f.Stat()
    old_size = info.Size()
    if err != nil {
        return errors.Wrap(err, "Could not obtain the stats for metadata lookup file")
    }
    //If the file needs to be truncated, do that
    if  old_size < min_req_size {
        err = f.Truncate(min_req_size)
        if err != nil {
            return errors.Wrap(err, "Could not truncate metadata lookup file")
        }
    }
    //Add it to the metadata struct
    metadata.lookup_file = f
    //Create mmap
    var d []byte
    if old_size < min_req_size {
    d, err = syscall.Mmap(int(f.Fd()), 0, int(min_req_size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
} else {
    d, err = syscall.Mmap(int(f.Fd()), 0, int(old_size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
}
    if err != nil {
        return errors.Wrap(err, "Could not successfully create memory mapping over metadata lookup file")
    }
    metadata.lookup = d
    //Initialize the file
    metadata.initializeLookupFile(old_size, min_req_size)
    
    /*
    //Check if the file exists
    if _, err := os.Stat(path + metadataLookupFileName); os.IsNotExist(err) {
        //If it doesn't, create one
        f, err = os.OpenFile(path + metadataLookupFileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0777)
        if err != nil {
            return errors.Wrap(err, "Could not create metadata lookup file")
        }
        //new_size = int64(math.Ceil(float64(metadata_length)/float64(metdataIntervalLength)))
        err = f.Truncate(min_req_size)
        if err != nil {
            return errors.Wrap(err, "Could not truncate metadata lookup file")
        }
    } else {
        //If it does, open it
        f, err = os.OpenFile(path + metadataLookupFileName, os.O_RDWR|os.O_APPEND, 0777)
        if err != nil {
            return errors.Wrap(err, "Could not open metadata lookup file")
        }
        //Check the size of the lookup file, and if it is less than the required value, extend it
        info, err := f.Stat()
        old_size = info.Size()
        if err != nil {
            return errors.Wrap(err, "Could not obtain the stats for metadata lookup file")
        }
        if  old_size < min_req_size {
            err = f.Truncate(min_req_size)
            if err != nil {
                return errors.Wrap(err, "Could not truncate metadata lookup file")
            }
        }
    }
    metadata.lookup_file = f
    //Create mmap
    d, err := syscall.Mmap(int(f.Fd()), 0, int(new_size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
    if err != nil {
        return errors.Wrap(err, "Could not successfully create memory mapping over metadata lookup file")
    }
    metadata.lookup = d
    //Fill up remaining bytes with 0
    for i:=old_size; i<new_size; i++ {
        metadata.lookup[i] = byte(0x00)
    } */
    return nil
}

func (metadata *Metadata) initializeLookupFile (old_size, new_size int64) {
    for i:=old_size; i<new_size; i++ {
        metadata.lookup[i] = byte(0x00)
    }
}

/*
func (m *Metadata) reserveSlot() (uint64, err) {
   //Iterate through lookup file to find open slots
   //TODO: parallelize this
   for i:=0; i<m.lookup_size; i++ {
        //If byte is not 0xff, there is an open slot
        if lookup[i] != 0xff {
            //Check to see which slot is open
            for j, v := range SLOT_OCCUPIED_VALUES {
                if t:=lookup[i] & v; t == 0x00 {
                    //Slot j is open

                }
            }
        }
   }
   //If there are no free slots available
   return nil, nil
}
*/
