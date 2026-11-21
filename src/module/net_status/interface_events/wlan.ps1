# Runs in a dedicated Windows PowerShell process. Only normalized fields are
# written to stdout. EventLogWatcher invokes handlers on a CLR worker thread.
$ErrorActionPreference = 'Stop'
try {
    Add-Type -TypeDefinition @'
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics.Eventing.Reader;
using System.Globalization;
using System.Threading;

public sealed class OpenNetWlan
{
    private const string Channel = "Microsoft-Windows-WLAN-AutoConfig/Operational";
    private const string Filter = "*[System[Provider[@Name='Microsoft-Windows-WLAN-AutoConfig'] and (EventID=8001 or EventID=8003)]]";
    private static readonly CultureInfo Culture = CultureInfo.InvariantCulture;

    public static string Project(int id, object identifier, DateTime? time, object reason)
    {
        if (id != 8001 && id != 8003) return null;
        Guid guid;
        if (!Guid.TryParse(Convert.ToString(identifier, Culture), out guid) || guid == Guid.Empty)
            return "GAP\t-";

        string code = "-";
        string text = Convert.ToString(reason, Culture);
        uint number;
        if (id == 8003 && text != null)
        {
            bool parsed = text.StartsWith("0x", StringComparison.OrdinalIgnoreCase)
                ? UInt32.TryParse(text.Substring(2), NumberStyles.AllowHexSpecifier, Culture, out number)
                : UInt32.TryParse(text, NumberStyles.None, Culture, out number);
            if (parsed) code = number.ToString(Culture);
        }
        string timestamp = "-";
        if (time.HasValue)
        {
            var milliseconds = time.Value.ToUniversalTime().Ticks / TimeSpan.TicksPerMillisecond;
            var epoch = new DateTime(1970, 1, 1).Ticks / TimeSpan.TicksPerMillisecond;
            timestamp = (milliseconds - epoch).ToString(Culture);
        }
        return String.Join("\t", "WLAN", id.ToString(Culture), guid.ToString("D"), timestamp, code);
    }

    private static string ErrorCode(Exception error)
    {
        var native = error as Win32Exception;
        if (native != null) return native.NativeErrorCode.ToString(Culture);
        // Some Framework exceptions only expose a generic CLR HRESULT.
        if (((uint)error.HResult >> 16) == 0x8007)
            return (error.HResult & 0xffff).ToString(Culture);
        return "-";
    }

    // The watcher can deliver events synchronously while it is being enabled.
    // Bound this queue, send READY first, and preserve a gap after older events.
    private readonly Queue<string> pending = new Queue<string>();
    private bool overflow;
    private bool stopped;
    private string failure;

    private void Publish(string line)
    {
        if (line == null) return;
        lock (pending)
        {
            if (stopped) return;
            if (pending.Count >= 128) overflow = true;
            if (!overflow) pending.Enqueue(line);
            Monitor.Pulse(pending);
        }
    }

    private void Stop(string code)
    {
        lock (pending)
        {
            stopped = true;
            if (code != null) failure = code;
            Monitor.Pulse(pending);
        }
    }

    private void Drain()
    {
        while (true)
        {
            string line;
            lock (pending)
            {
                while (pending.Count == 0 && !overflow && !stopped) Monitor.Wait(pending);
                if (pending.Count != 0) line = pending.Dequeue();
                else if (overflow) { overflow = false; line = "GAP\t-"; }
                else if (failure != null) { line = "ERROR\t" + failure; failure = null; }
                else return;
            }
            Console.WriteLine(line);
        }
    }

    private void Collect()
    {
        using (var config = new EventLogConfiguration(Channel))
        {
            if (!config.IsEnabled) { Console.WriteLine("ERROR\t-"); return; }
        }

        var query = new EventLogQuery(Channel, PathType.LogName, Filter);
        var paths = new [] { "Event/EventData/Data[@Name='InterfaceGuid']",
                            "Event/EventData/Data[@Name='ReasonCode']" };
        using (var fields = new EventLogPropertySelector(paths))
        using (var watcher = new EventLogWatcher(query, null, false))
        {
            watcher.EventRecordWritten += delegate(object sender, EventRecordWrittenEventArgs args)
            {
                if (args.EventException != null) { Stop(ErrorCode(args.EventException)); return; }
                try
                {
                    using (var record = args.EventRecord)
                    {
                        var native = record as EventLogRecord;
                        if (native == null) { Publish("GAP\t-"); return; }
                        var values = native.GetPropertyValues(fields);
                        object guid = values.Count > 0 ? values[0] : null;
                        object reason = values.Count > 1 ? values[1] : null;
                        Publish(Project(record.Id, guid, record.TimeCreated, reason));
                    }
                }
                catch (Exception error) { Publish("GAP\t" + ErrorCode(error)); }
            };
            watcher.Enabled = true;
            lock (pending)
            {
                if (failure != null) { Console.WriteLine("ERROR\t" + failure); return; }
            }
            Console.WriteLine("READY");
            ThreadPool.QueueUserWorkItem(delegate
            {
                try { while (Console.In.Read() != -1) {} }
                catch (Exception error) { Stop(ErrorCode(error)); return; }
                Stop(null);
            });
            Drain();
        }
    }

    public static void Run()
    {
        try { new OpenNetWlan().Collect(); }
        catch (Exception error) { Console.WriteLine("ERROR\t" + ErrorCode(error)); }
    }
}
'@
    [OpenNetWlan]::Run()
} catch {
    [Console]::WriteLine("ERROR`t-")
}
