package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 *	This class  describes compiled constants that are passed into
 *	Updatable VTIResultSets.
 *
 */

public class UpdatableVTIConstantOperation extends WriteCursorConstantOperation {

	/********************************************************
	**
	**	This class implements Formatable. But it is NOT used
 	**	across either major or minor releases.  It is only
	** 	written persistently in stored prepared statements, 
	**	not in the replication stage.  SO, IT IS OK TO CHANGE
	**	ITS read/writeExternal.
	**
	********************************************************/
	public int[]	changedColumnIds;
    public int statementType;

	/**
	 * Public niladic constructor. Needed for Formatable interface to work.
	 *
	 */
    public	UpdatableVTIConstantOperation() { 
    	super(); 
    }

	/**
	 *	Make the ConstantAction for an updatable VTI statement.
	 *
	 * @param deferred					Whether or not to do operation in deferred mode
     * @param changedColumnIds Array of ids of changed columns
	 *
	 */
	@SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
	public	UpdatableVTIConstantOperation( int statementType, boolean deferred, int[] changedColumnIds) {
		super(0, null, null, null, null, null, null, deferred, null, null, 0, null,	null,
				null, null, null, null, false);
        this.statementType = statementType;
        this.changedColumnIds = changedColumnIds;
	}

	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ 
		return StoredFormatIds.UPDATABLE_VTI_CONSTANT_ACTION_V01_ID; 
	}
}
