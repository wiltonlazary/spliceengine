package com.splicemachine.derby.utils.marshall;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.uuid.Snowflake;
import com.splicemachine.uuid.UUIDGenerator;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

/**
 * Postfix which uses UUIDs to ensure that the postfix is unique.
 *
 * @author Scott Fines
 *         Date: 11/18/13
 */
public class UniquePostfix implements KeyPostfix{
    private final byte[] baseBytes;
    private final UUIDGenerator generator;

    public UniquePostfix(byte[] baseBytes){
        this(baseBytes,EngineDriver.driver().newUUIDGenerator(100));
    }

    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
    public UniquePostfix(byte[] baseBytes,UUIDGenerator generator){
        this.generator=generator;
        this.baseBytes=baseBytes;
    }

    @Override
    public int getPostfixLength(byte[] hashBytes) throws StandardException{
        return baseBytes.length+Snowflake.UUID_BYTE_SIZE;
    }

    @Override
    public void encodeInto(byte[] keyBytes,int postfixPosition,byte[] hashBytes){
        byte[] uuidBytes=generator.nextBytes();
        System.arraycopy(uuidBytes,0,keyBytes,postfixPosition,uuidBytes.length);
        System.arraycopy(baseBytes,0,keyBytes,postfixPosition+uuidBytes.length,baseBytes.length);
    }

    @Override
    public void close() throws IOException{
    }
}
