//This package deals with writing, reading, updating, deleting and providing related operations on the metadata file.
//Each record in this file is of fixed length and is uniquely identified by its slot number
package metadataHandler

import (
    "github.com/renjithwarrier94/disk_store/logger"
    "os"
    "fmt"
    "syscall"
    "encoding/gob"
    "bytes"
    "errors"
    "math"
)

//The interval to leave for each slot data
const metdataIntervalLength = 200
//The name of the metadata file. Its stored in the given path + fileName
const metadataFileName = "metadata.ds"
//The name of the metadata lookup file.
const metadataLookupFileName = "metadata_lookup.ds"
//All the status byte flags
const (
    SLOT_IN_USE                 = byte(0x01)
    SLOT_BEING_MODIFIED         = byte(0x02)
    SLOT_MARKED_FOR_DELETION    = byte(0x04)
)

type Metadata struct {
    data            []byte
    file            *os.File
    currentSlot     *Slot
    currentSlotNo   uint64
    lookup          []byte
    lookup_file     *os.File
    log             *logger.Logger
}

//Get the metadata type for the metadata file mapped to a byte slice
//Pass the path where the data files need to live/exists and the size in bytes
//If the size is less than 4096 bytes (4 KB), 4096 is used
//If it is more than 4096, the nearest multiple of 4096 ie ceil(fileSize/4096)*4096 is used
func GetMetadata(path string, fileSize int64) (*Metadata, error) {
    metadata := Metadata{log: logger.GetLogger(true)}
    fileSize = int64(math.Ceil(float64(fileSize)/4096.0) * 4096)
    f, err := os.OpenFile(path + metadataFileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0777)
    if err != nil {
        return nil, err
    }
    //Add File type to metadata
    metadata.file = f
    info, err := f.Stat()
    if err != nil {
        return nil, err
    }
    if info.Size() < fileSize {
        err = f.Truncate(fileSize)
        if err != nil {
            return nil, err
        }
    } else {
        fileSize = info.Size()
    }

    //Get memory mapped byte slice
    d, err := syscall.Mmap(int(f.Fd()), 0, int(fileSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
    if err != nil {
        return nil, err
    }
    //Add data to metadata object
    metadata.data = d
    //Create/Open the lookup file
    err = metadata.createMetadataLookupFile(path, fileSize)
    if err != nil {
        return nil, err
    }
    return &metadata, nil
}

//The slot is written at the location slotNo*metdataIntervalLength
//The first byte is the status byte. It is the ORed value of the SLOT_* constants (whichever applicable)
//Rest of it is the slot data
func (m Metadata) WriteSlot(s Slot, slotNo uint64) error {
    writeOffset := slotNo * metdataIntervalLength
    //TODO: MAKE THE OPERATION ATOMIC
    //Set the status byte to show the slot is in use and is being modified
    statusByte := SLOT_IN_USE | SLOT_BEING_MODIFIED
    m.data[writeOffset] = statusByte

    writeSlice := m.data[writeOffset+1:writeOffset+metdataIntervalLength]
    var buffer bytes.Buffer
    encoder := gob.NewEncoder(&buffer)
    err := encoder.Encode(s)
    if err != nil {
        return err
    }
    readSlice := buffer.Bytes()
    n := copy(writeSlice, readSlice)
    if tot := len(readSlice); n != tot {
        return errors.New(fmt.Sprintf("Could not write the entire metadata. Could only write %v of %v bytes", n, tot))
    }

    //Set the status byte to show that the slot is in use only
    statusByte = SLOT_IN_USE
    m.data[writeOffset] = statusByte
    return nil
}

func (m Metadata) CloseFile() error {
    err := m.file.Sync()
    if err != nil {
        return err
    }
    err = m.file.Close()
    if err != nil {
        return err
    }
    return nil
}

func (m Metadata) GetSlot(slotno uint64) (Slot, error) {
    readOffset := slotno * metdataIntervalLength
    //1st Byte is the status byte. Read from the next byte for slot data
    readSlice := m.data[readOffset+1: readOffset+metdataIntervalLength]
    var decodedSlot Slot
    buffer := bytes.NewBuffer(readSlice)
    decoder := gob.NewDecoder(buffer)
    err := decoder.Decode(&decodedSlot)
    if err != nil {
        return decodedSlot, err
    }
    return decodedSlot, nil
}

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
            return err
        }
        //Set the size to the min required size
        new_size = int64(math.Ceil(float64(metadata_length)/float64(metdataIntervalLength)))
        err = f.Truncate(new_size)
        if err != nil {
            return err
        }
    } else {
        //If it does, open it
        f, err = os.OpenFile(path + metadataLookupFileName, os.O_RDWR|os.O_APPEND, 0777)
        if err != nil {
            return err
        }
        //Check the size of the lookup file, and if it is less than the required value, extend it
        info, err := f.Stat()
        old_size = info.Size()
        if err != nil {
            return err
        }
        if min_req_size := int64(math.Ceil(float64(metadata_length)/float64(metdataIntervalLength))); info.Size() < min_req_size {
            err = f.Truncate(min_req_size)
            if err != nil {
                return err
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
        return err
    }
    metadata.lookup = d
    //Fill up remaining bytes with 0
    for i:=old_size; i<new_size; i++ {
        metadata.lookup[i] = byte(0x00)
    }
    return nil
}
