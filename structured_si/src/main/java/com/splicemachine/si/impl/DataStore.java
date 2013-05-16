package com.splicemachine.si.impl;

import com.splicemachine.si.api.TransactionId;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.SGet;
import com.splicemachine.si.data.api.SRead;
import com.splicemachine.si.data.api.STable;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.splicemachine.constants.SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME;
import static com.splicemachine.constants.SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE;

/**
 * Library of functions used by the SI module when accessing rows from data tables (data tables as opposed to the
 * transaction table).
 */
public class DataStore {
    private final SDataLib dataLib;
    private final STableReader reader;
    private final STableWriter writer;

    private final String siNeededAttribute;
    private final Object siNeededValue;
    private final Object onlySIFamilyNeededValue;
    private final String transactionIdAttribute;
    private final String deletePutAttribute;

    private final Object siFamily;
    private final Object commitTimestampQualifier;
    private final Object tombstoneQualifier;
    private final Object siNull;
    final Object siFail;

    private final Object userColumnFamily;

    public DataStore(SDataLib dataLib, STableReader reader, STableWriter writer, String siNeededAttribute,
                     Object siNeededValue, Object onlySIFamilyNeededValue,
                     String transactionIdAttribute, String deletePutAttribute,
                     String siMetaFamily, Object siCommitQualifier, Object siTombstoneQualifier, Object siMetaNull,
                     Object siFail, Object userColumnFamily) {
        this.dataLib = dataLib;
        this.reader = reader;
        this.writer = writer;
        this.siNeededAttribute = siNeededAttribute;
        this.siNeededValue = dataLib.encode(siNeededValue);
        this.onlySIFamilyNeededValue = dataLib.encode(onlySIFamilyNeededValue);
        this.transactionIdAttribute = transactionIdAttribute;
        this.deletePutAttribute = deletePutAttribute;
        this.siFamily = dataLib.encode(siMetaFamily);
        this.commitTimestampQualifier = dataLib.encode(siCommitQualifier);
        this.tombstoneQualifier = dataLib.encode(siTombstoneQualifier);
        this.siNull = dataLib.encode(siMetaNull);
        this.siFail = dataLib.encode(siFail);
        this.userColumnFamily = dataLib.encode(userColumnFamily);
    }

    void setSiNeededAttribute(Object put, boolean siFamilyOnly) {
        dataLib.addAttribute(put, siNeededAttribute, dataLib.encode(siFamilyOnly ? onlySIFamilyNeededValue : siNeededValue));
    }

    Object getSiNeededAttribute(Object put) {
        return dataLib.getAttribute(put, siNeededAttribute);
    }

    boolean isSIFamilyOnly(Object put) {
        return dataLib.valuesEqual(dataLib.getAttribute(put, siNeededAttribute), onlySIFamilyNeededValue);
    }

    void setDeletePutAttribute(Object put) {
        dataLib.addAttribute(put, deletePutAttribute, dataLib.encode(true));
    }

    Boolean getDeletePutAttribute(Object put) {
        Object neededValue = dataLib.getAttribute(put, deletePutAttribute);
        return (Boolean) dataLib.decode(neededValue, Boolean.class);
    }

    void addTransactionIdToPut(Object put, TransactionId transactionId) {
        dataLib.addKeyValueToPut(put, siFamily, commitTimestampQualifier, transactionId.getId(), siNull);
    }

    void setTransactionId(SITransactionId transactionId, Object operation) {
        dataLib.addAttribute(operation, transactionIdAttribute, dataLib.encode(transactionId.getTransactionIdString()));
    }

    SITransactionId getTransactionIdFromOperation(Object put) {
        Object value = dataLib.getAttribute(put, transactionIdAttribute);
        String transactionId = (String) dataLib.decode(value, String.class);
        if (transactionId != null) {
            return new SITransactionId(transactionId);
        }
        return null;
    }

    void copyPutKeyValues(Object put, Object newPut, long timestamp) {
        for (Object keyValue : dataLib.listPut(put)) {
            dataLib.addKeyValueToPut(newPut, dataLib.getKeyValueFamily(keyValue),
                    dataLib.getKeyValueQualifier(keyValue),
                    timestamp,
                    dataLib.getKeyValueValue(keyValue));
        }
    }

    List getCommitTimestamp(STable table, Object rowKey) throws IOException {
        final List<List<Object>> columns = Arrays.asList(Arrays.asList(siFamily, commitTimestampQualifier));
        SGet get = dataLib.newGet(rowKey, null, columns, null);
        Object result = reader.get(table, get);
        if (result != null) {
            return dataLib.getResultColumn(result, siFamily, commitTimestampQualifier);
        }
        return null;
    }

    public KeyValueType getKeyValueType(Object family, Object qualifier) {
        if (dataLib.valuesEqual(family, siFamily) && dataLib.valuesEqual(qualifier, commitTimestampQualifier)) {
            return KeyValueType.COMMIT_TIMESTAMP;
        } else if (dataLib.valuesEqual(family, siFamily) && dataLib.valuesEqual(qualifier, tombstoneQualifier)) {
            return KeyValueType.TOMBSTONE;
        } else if (dataLib.valuesEqual(family, userColumnFamily)) {
            return KeyValueType.USER_DATA;
        } else {
            return KeyValueType.OTHER;
        }
    }

    public boolean isSiNull(Object value) {
        return dataLib.valuesEqual(value, siNull);
    }

    public boolean isSiFail(Object value) {
        return dataLib.valuesEqual(value, siFail);
    }

    public void recordRollForward(RollForwardQueue rollForwardQueue, ImmutableTransaction transaction, Object row) {
        recordRollForward(rollForwardQueue, transaction.beginTimestamp, row);
    }

    public void recordRollForward(RollForwardQueue rollForwardQueue, long beginTimestamp, Object row) {
        if (rollForwardQueue != null) {
            rollForwardQueue.recordRow(beginTimestamp, row);
        }
    }

    public void setCommitTimestamp(STable table, Object rowKey, long beginTimestamp, long commitTimestamp) throws IOException {
        setCommitTimestampDirect(table, rowKey, beginTimestamp, dataLib.encode(commitTimestamp));
    }

    public void setCommitTimestampToFail(STable table, Object rowKey, long beginTimestamp) throws IOException {
        setCommitTimestampDirect(table, rowKey, beginTimestamp, siFail);
    }

    private void setCommitTimestampDirect(STable table, Object rowKey, long beginTimestamp, Object timestampValue) throws IOException {
        Object put = dataLib.newPut(rowKey);
        suppressIndexing(put);
        dataLib.addKeyValueToPut(put, siFamily, commitTimestampQualifier, beginTimestamp, timestampValue);
        writer.write(table, put, false);
    }

    /**
     * When this new operation goes through the co-processor stack it should not be indexed (because it already has been
     * when the original operation went through).
     */
    public void suppressIndexing(Object newPut) {
        dataLib.addAttribute(newPut, SUPPRESS_INDEXING_ATTRIBUTE_NAME, SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
    }

    public void setTombstoneOnPut(Object put, SITransactionId transactionId) {
        dataLib.addKeyValueToPut(put, siFamily, tombstoneQualifier, transactionId.getId(), siNull);
    }

    public void setTombstonesOnColumns(STable table, long timestamp, Object put) throws IOException {
        final Map userData = getUserData(table, dataLib.getPutKey(put));
        if (userData != null) {
            for (Object qualifier : userData.keySet()) {
                dataLib.addKeyValueToPut(put, userColumnFamily, qualifier, timestamp, siNull);
            }
        }
    }

    private Map getUserData(STable table, Object rowKey) throws IOException {
        final List<Object> families = Arrays.asList(userColumnFamily);
        SGet get = dataLib.newGet(rowKey, families, null, null);
        dataLib.setReadMaxVersions(get, 1);
        Object result = reader.get(table, get);
        if (result != null) {
            return dataLib.getResultFamilyMap(result, userColumnFamily);
        }
        return null;
    }

    public void addSiFamilyToRead(SRead read) {
        dataLib.addFamilyToRead(read, siFamily);
    }

    public void addSiFamilyToReadIfNeeded(SRead read) {
        dataLib.addFamilyToReadIfNeeded(read, siFamily);
    }
}
