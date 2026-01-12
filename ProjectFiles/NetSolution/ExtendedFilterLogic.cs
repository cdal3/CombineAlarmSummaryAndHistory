#region Using directives
using System;
using UAManagedCore;
using OpcUa = UAManagedCore.OpcUa;
using FTOptix.UI;
using FTOptix.HMIProject;
using FTOptix.NativeUI;
using FTOptix.Retentivity;
using FTOptix.CoreBase;
using FTOptix.Core;
using FTOptix.NetLogic;
using FTOptix.Alarm;
using FTOptix.SerialPort;
using FTOptix.SQLiteStore;
using FTOptix.Store;
using FTOptix.EventLogger;
#endregion

public class ExtendedFilterLogic : BaseNetLogic
{
    public override void Start()
    {
        // Insert code to be executed when the user-defined logic is started
    }

    public override void Stop()
    {
        // Insert code to be executed when the user-defined logic is stopped
    }

    [ExportMethod]
    public void FilterOnlyActiveAlarms()
    {
        var store = Project.Current.Get<Store>("DataStores/EmbeddedDatabase1");
        var startTime = ((DateTime)Owner.Owner.GetVariable("From").Value).ToString("yyyy-MM-dd HH:mm:ss");
        var endTime = ((DateTime)Owner.Owner.GetVariable("To").Value).ToString("yyyy-MM-dd HH:mm:ss");
        var dataGrid = Owner as DataGrid;
        
        // Drop tables if they exist
        try { store.Query("DROP TABLE FilteredAlarms", out _, out _); } catch { }
        
        // Create result table with JOIN and NULL handling via store.Query
        string createResultTable = $@"CREATE TABLE FilteredAlarms AS 
            SELECT t1.*, 
                   CASE WHEN t2.CurrentState IS NULL THEN 0 ELSE t2.CurrentState END AS CurrentState 
            FROM AlarmsEventLogger1 AS t1 
            LEFT JOIN (
                SELECT ConditionName AS AlarmName, 
                       MAX(LocalTime) AS MaxTime, 
                       ActiveState_Id AS CurrentState 
                FROM AlarmsEventLogger1 
                GROUP BY ConditionName
            ) AS t2 
            ON t1.ConditionName = t2.AlarmName 
            AND t1.LocalTime = t2.MaxTime 
            AND t1.LocalTime BETWEEN '{startTime}' AND '{endTime}'";
        
        try
        {{
            store.Query(createResultTable, out _, out _);
            Log.Info("ExtendedFilterLogic", "Created FilteredAlarms table");
        }}
        catch (Exception ex)
        {{
            Log.Error("ExtendedFilterLogic", $"Failed to create FilteredAlarms table: {{ex.Message}}");
            // Fallback: try without CASE
            try {{ store.Query("DROP TABLE FilteredAlarms", out _, out _); }} catch {{ }}
            string fallbackQuery = $@"CREATE TABLE FilteredAlarms AS 
                SELECT t1.* 
                FROM AlarmsEventLogger1 AS t1 
                WHERE t1.LocalTime BETWEEN '{startTime}' AND '{endTime}'";
            store.Query(fallbackQuery, out _, out _);
        }}
              
        // Simple query for DataGrid
        string query = "SELECT * FROM FilteredAlarms";
        
        Log.Info("ExtendedFilterLogic", $"Setting query: {{query}}");
        dataGrid.Query = query;
        dataGrid.Refresh();
        
        // Clean up
        try {{ store.Query("DROP TABLE FilteredAlarms", out _, out _); }} catch {{ }}
    }
}
