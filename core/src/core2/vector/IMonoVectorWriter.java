package core2.vector;

import java.nio.ByteBuffer;

@SuppressWarnings("try")
public interface IMonoVectorWriter extends IVectorWriter2 {

    void writeNull(Void nullValue);

    void writeBoolean(boolean booleanValue);
    void writeByte(byte byteValue);
    void writeShort(short shortValue);
    void writeInt(int intValue);
    void writeLong(long longValue);

    void writeFloat(float floatValue);
    void writeDouble(double doubleValue);

    void writeBuffer(ByteBuffer bufferValue);
    void writeObject(Object objectValue);
}
