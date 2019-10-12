package metadataHandler

//All methods related to metadataLookup file

import (
    "os"
    "syscall"
    "github.com/pkg/errors"
    "math"
)

//Create/open a metadata lookup file
func (metadata *Metadata) createMetadataLookupFile(path string, metadata_length int64) error {
    var f *os.File
    //To fill up the lookup file with empty bytes, we need the old and new size. For new file, old_size is 0
    var old_size, new_size int64
    //Check if the file exists
    if _, err := os.Stat(path + metadataLookupFileName); os.IsNotExist(err) {
        //If it doesn't, create one
        f, err = os.OpenFile(path + metadataLookupFileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0777)
        if err != nil {
            return errors.Wrap(err, "Could not create metadata lookup file")
        }
        //Set the size to the min required size
        new_size = int64(math.Ceil(float64(metadata_length)/float64(metdataIntervalLength)))
        err = f.Truncate(new_size)
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
        if min_req_size := int64(math.Ceil(float64(metadata_length)/float64(metdataIntervalLength))); info.Size() < min_req_size {
            err = f.Truncate(min_req_size)
            if err != nil {
                return errors.Wrap(err, "Could not truncate metadata lookup file")
            }
            new_size = min_req_size
        } else {
            new_size = info.Size()
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
    }
    return nil
}


