//This package deals with writing, reading, updating, deleting and providing related operations on the metadata file.
//Each record in this file is of fixed length and is uniquely identified by its slot number
package metadataHandler

import (
    "github.com/renjithwarrier94/disk_store/logger"
    "os"
    "fmt"
    "syscall"
    "encoding/gob"
    "encoding/binary"
    "bytes"
    "github.com/pkg/errors"
    "math"
    "unsafe"
    "sync/atomic"
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
    lookup          []byte
    lookup_file     *os.File
    log             *logger.Logger
    byteOrder       binary.ByteOrder
    //lookup_size     uint64
    //data_size       uint64
    num_slots       uint32
}

//Get the byte order (big or little endian) of the current architecture
func getByteOrder() binary.ByteOrder {
    buf := [2]byte{}
    *(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xABCD)

    switch buf {
        case [2]byte{0xCD, 0xAB}: return binary.LittleEndian
        case [2]byte{0xAB, 0xCD}: return binary.BigEndian
        default:                return nil
    }
}

//Get the metadata type for the metadata file mapped to a byte slice
//Pass the path where the data files need to live/exists and the size in bytes
//If the size is less than 4096 bytes (4 KB), 4096 is used
//If it is more than 4096, the nearest multiple of 4096 ie ceil(fileSize/4096)*4096 is used
func GetMetadata(path string, fileSize int64) (*Metadata, error) {
    var old_size int64
    metadata := Metadata{log: logger.GetLogger(true)}
    fileSize = int64(math.Ceil(float64(fileSize)/4096.0) * 4096)
    f, err := os.OpenFile(path + metadataFileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0777)
    if err != nil {
        return nil, errors.Wrap(err, "Could not open/create the metadata file")
    }
    //Add File type to metadata
    metadata.file = f
    info, err := f.Stat()
    if err != nil {
        return nil, errors.Wrap(err, "Could not fetch the stats of metadata file")
    }
    old_size = info.Size()
    if info.Size() < fileSize {
        err = f.Truncate(fileSize)
        if err != nil {
            return nil, errors.Wrap(err, "Could not truncate metadata file")
        }
    } else {
        fileSize = info.Size()
    }

    //Get memory mapped byte slice
    d, err := syscall.Mmap(int(f.Fd()), 0, int(fileSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
    if err != nil {
        return nil, errors.Wrap(err, "Could not create a memory mapping over metadata file")
    }
    //Add data to metadata object
    metadata.data = d
    //Set num slots
    metadata.num_slots = uint32(math.Floor(float64( fileSize )/float64( metdataIntervalLength )))
    //Create/Open the lookup file
    err = metadata.createMetadataLookupFile(path)
    if err != nil {
        return nil, errors.Wrap(err, "Could not successfully create the metadata lookup file")
    }
    //Initialize with null values
    metadata.initializeMetadataFile(old_size, fileSize)
    //Set the data size
   // metadata.data_size = fileSize
    //Set byte order in metadata
    metadata.byteOrder = getByteOrder()
    if metadata.byteOrder == nil {
        return nil, errors.Errorf("Could not fetch the byte order used in the current system architecture")
    }
    return &metadata, nil
}

//Set initial bytes in metadata file
func (m *Metadata) initializeMetadataFile(old_size, new_size int64) {
    for i:=old_size; i<new_size; i++ {
        m.data[i] = 0x00
    }
}

//Check whether the slot is free for use and set its initial status byte
func (m *Metadata) checkAndSetStatusByteBeforeWrite(slotNo uint64, checkExisting bool) ( bool, error ) {
    current_slot_offset := slotNo * metdataIntervalLength
    initial_status_byte := SLOT_IN_USE | SLOT_BEING_MODIFIED
    //Get the 1st 4 bytes from the slot
    first_4_bytes := make([]byte, 4)
    n := copy(first_4_bytes, m.data[current_slot_offset: current_slot_offset + 4])
    if n != 4 {
        return false, errors.Errorf("Could not copy the first 4 bytes in the slot")
    }
    //Convert it to uint32
    old_data := m.byteOrder.Uint32(first_4_bytes)
    //Set the new status byte
    first_4_bytes[0] = initial_status_byte
    //Convert this to uint32
    new_data := m.byteOrder.Uint32(first_4_bytes)
    //Convert the slot start pointer to *uint32
    var ptr *uint32 = (*uint32)(unsafe.Pointer(&m.data[current_slot_offset]))
    if !checkExisting {
        //Just write
        res := atomic.CompareAndSwapUint32(ptr, old_data, new_data)
        return res, nil
    } else {
        //Check whether the old status byte is 0x00 or slot in use
        old_status_byte := m.data[current_slot_offset]
        if old_status_byte == 0x00 || old_status_byte == initial_status_byte {
            res := atomic.CompareAndSwapUint32(ptr, old_data, new_data)
            return res, nil
        }
        return false, errors.Errorf("Initial status byte of the metadata slot is not right. (%v)", old_status_byte)
    }
}

//Unset the status byte of metadata slot
func (m *Metadata) checkAndUnsetStatusByteAfterWrite(slotNo uint64) {
    current_slot_offset := slotNo * metdataIntervalLength
    //Get the current staatus byte
    old_status_byte := m.data[current_slot_offset]
    new_status_byte := old_status_byte & ^old_status_byte
    m.data[current_slot_offset] = new_status_byte
}

//The slot is written at the location slotNo*metdataIntervalLength
//The first byte is the status byte. It is the ORed value of the SLOT_* constants (whichever applicable)
//Rest of it is the slot data
func (m *Metadata) WriteSlot(s Slot, slotNo uint64) error {
    writeOffset := slotNo * metdataIntervalLength
    //Set the status byte to show the slot is in use and is being modified
    //TODO: retries when it returns false
    res, _ := m.checkAndSetStatusByteBeforeWrite(slotNo, true)
    if !res {
        return errors.Errorf("Could not write status byte as result is %v ", res)
    }

    writeSlice := m.data[writeOffset+1:writeOffset+metdataIntervalLength]
    var buffer bytes.Buffer
    encoder := gob.NewEncoder(&buffer)
    err := encoder.Encode(s)
    if err != nil {
        return errors.Wrap(err, "Could not successfully encode slice")
    }
    readSlice := buffer.Bytes()
    n := copy(writeSlice, readSlice)
    if tot := len(readSlice); n != tot {
        return errors.Errorf(fmt.Sprintf("Could not write the entire metadata. Could only write %v of %v bytes", n, tot))
    }

    //Set the status byte to show that the slot is in use only
    m.checkAndUnsetStatusByteAfterWrite(slotNo)
    return nil
}

func (m *Metadata) CloseFile() error {
    //Close metadata file
    err := m.file.Sync()
    if err != nil {
        return errors.Wrap(err, "Could not sync metadata file before closing")
    }
    err = m.file.Close()
    if err != nil {
        return errors.Wrap(err, "Could not close metadata file")
    }
    //Close metadata lookup file
    err = m.lookup_file.Sync()
    if err != nil {
        return errors.Wrap(err, "Could not sync metadata lookup file before closing")
    }
    err = m.lookup_file.Close()
    if err != nil {
        return errors.Wrap(err, "Could not close metadata lookup file")
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
        return decodedSlot, errors.Wrap(err, "Error in decoding slice")
    }
    return decodedSlot, nil
}
