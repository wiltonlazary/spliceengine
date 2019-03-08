package com.splicemachine.derby.stream.function;

import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.stream.ActivationHolder;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.si.constants.SIConstants;
import org.apache.hadoop.hbase.KeyValue;

import java.util.List;

/**
 * Created by jyuan on 3/7/19.
 */
public class BulkLoadIndexHFileGenerationFunction extends HFileGenerationFunction {

    public BulkLoadIndexHFileGenerationFunction() {}

    public BulkLoadIndexHFileGenerationFunction(OperationContext operationContext,
                                                long txnId,
                                                Long heapConglom,
                                                String compressionAlgorithm,
                                                List<BulkImportPartition> partitionList,
                                                String tableVersion,
                                                DDLMessage.TentativeIndex tentativeIndexList,
                                                ActivationHolder ah) {
        super(operationContext, txnId, heapConglom, compressionAlgorithm, partitionList, tableVersion, tentativeIndexList);
        operationType = OperationType.CREATE_INDEX;
        this.ah = ah;
    }

    @Override
    protected void writeToHFile (byte[] rowKey, byte[] value) throws Exception {
        KeyValue kv = new KeyValue(rowKey, SIConstants.DEFAULT_FAMILY_BYTES,
                SIConstants.PACKED_COLUMN_BYTES, txnId, value);
        writer.append(kv);

    }
}
