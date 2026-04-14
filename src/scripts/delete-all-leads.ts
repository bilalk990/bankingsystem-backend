import { db, pool } from '../configs/database';
import { leadsTable, documentsTable, bankStatementAnalysesTable, underwritingConfirmationsTable, tasksTable, smartRemindersTable, leadMessagesTable, callsTable, dealsTable, activitiesTable, scrubTracesTable, renewalSuggestionsTable } from '../models';

async function deleteAllLeads() {
  console.log("Starting bulk delete of all leads...");

  try {
    // Delete all leads. Because of cascading deletes, this should clean up all related tables.
    const result = await db.delete(leadsTable).returning();

    console.log(`Successfully deleted ${result.length} leads.`);
    console.log("Cleanup of related records completed via database cascades.");

  } catch (error) {
    console.error("Error during bulk delete:", error);
  } finally {
    await pool.end();
  }
}

deleteAllLeads();
