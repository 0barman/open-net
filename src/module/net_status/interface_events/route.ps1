# Windows PowerShell 5.1. Each sample is scoped to the current compartment
# and active routes; an interface can have a different metric in each family.
function Select-OpenNetRoute($routes, $interfaces, $adapters, $prefix) {
    $byIndex = @{}
    foreach ($interface in $interfaces) { $byIndex[[uint32]$interface.InterfaceIndex] = $interface }
    $best = $null
    $winner = $null
    $tied = $false
    foreach ($route in $routes) {
        if ($route.DestinationPrefix -ne $prefix) { continue }
        if ($route.ValidLifetime -isnot [TimeSpan]) { return 'UNKNOWN' }
        if ($route.ValidLifetime -le [TimeSpan]::Zero) { continue }
        $index = [uint32]$route.InterfaceIndex
        $interface = $byIndex[$index]
        if ($null -eq $interface -or $null -eq $interface.ConnectionState) { return 'UNKNOWN' }
        if ([int]$interface.ConnectionState -eq 0) { continue }
        if ([int]$interface.ConnectionState -ne 1 -or $null -eq $route.RouteMetric -or
            $null -eq $interface.InterfaceMetric) { return 'UNKNOWN' }
        $cost = [uint64]$route.RouteMetric + [uint64]$interface.InterfaceMetric
        if ($null -eq $best -or $cost -lt $best) {
            $best = $cost
            $winner = $index
            $tied = $false
        } elseif ($cost -eq $best -and $index -ne $winner) { $tied = $true }
    }
    if ($null -eq $winner) { return 'NONE' }
    if ($tied) { return 'TIE' }
    $matching = @($adapters | Where-Object { $_.InterfaceIndex -eq $winner })
    if ($matching.Count -ne 1) { return 'UNKNOWN' }
    $guid = [Guid]::Empty
    if (![Guid]::TryParse([string]$matching[0].InterfaceGuid, [ref]$guid) -or $guid -eq [Guid]::Empty) {
        return 'UNKNOWN'
    }
    return $guid.ToString('D')
}

function Get-OpenNetRoute($family, $adapterCache) {
    $prefix = if ($family -eq 4) { '0.0.0.0/0' } else { '::/0' }
    try {
        $routes = @(NetTCPIP\Get-NetRoute -DestinationPrefix $prefix -PolicyStore ActiveStore -ErrorAction Stop)
    } catch {
        # The CDXML cmdlet reports an empty targeted query as ObjectNotFound.
        if ($_.CategoryInfo.Category -eq 'ObjectNotFound' -and
            $_.FullyQualifiedErrorId -like 'CmdletizationQuery_NotFound*,Get-NetRoute') { return 'NONE' }
        throw
    }
    if ($routes.Count -eq 0) { return 'NONE' }
    $indices = @($routes.InterfaceIndex | Select-Object -Unique)
    $interfaces = @(NetTCPIP\Get-NetIPInterface -AddressFamily "IPv$family" -InterfaceIndex $indices -PolicyStore ActiveStore -ErrorAction Stop)
    if (!$adapterCache.ContainsKey('rows')) {
        $adapterCache.rows = @(NetAdapter\Get-NetAdapter -IncludeHidden -ErrorAction Stop)
    }
    Select-OpenNetRoute $routes $interfaces $adapterCache.rows $prefix
}

function Get-OpenNetErrorCode($exception) {
    $cause = $exception.GetBaseException()
    if ($cause -is [ComponentModel.Win32Exception]) { return $cause.NativeErrorCode }
    if ($cause -is [System.Management.ManagementException]) { return [int]$cause.ErrorCode }
    if (($cause.HResult -band 0x7fff0000) -eq 0x70000) { return ($cause.HResult -band 0xffff) }
    return '-'
}

function Get-OpenNetRouteRecord($family, $adapterCache) {
    try {
        $state = Get-OpenNetRoute $family $adapterCache
        if ($state -eq 'UNKNOWN') { return "ROUTE`t$family`tUNKNOWN`t-" }
        return "ROUTE`t$family`t$state"
    } catch { return "ROUTE`t$family`tUNKNOWN`t$(Get-OpenNetErrorCode $_.Exception)" }
}

function Get-OpenNetRouteRecords {
    $adapterCache = @{}
    foreach ($family in 4,6) { Get-OpenNetRouteRecord $family $adapterCache }
}

function Write-OpenNetRouteObservations($watch) {
    while ($true) {
        Get-OpenNetRouteRecords | ForEach-Object { [Console]::WriteLine($_) }
        $signal = $watch.Wait()
        if ($signal -eq 'CLOSED') { return }
        if ($signal -ne 'REFRESH') {
            [Console]::WriteLine($signal)
            if ($signal.StartsWith("ERROR`t")) { return }
        }
    }
}

function Start-OpenNetRouteWatcher {
    $ErrorActionPreference = 'Stop'
    $ProgressPreference = 'SilentlyContinue'
    $WarningPreference = 'SilentlyContinue'
    $watch = $null
    try {
        Add-Type -ReferencedAssemblies System.dll -TypeDefinition @'
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Threading;

public sealed class OpenNetRouteWake : IDisposable
{
    // Each family owns one blocking wildcard notification. The sockets never
    // connect or send. All table reads and stdout writes run on the caller.
    private readonly object gate = new object();
    private readonly List<Socket> sockets = new List<Socket>(2);
    private readonly Stopwatch clock = Stopwatch.StartNew();
    private long lastWakeMs = -250;
    private bool refresh;
    private bool closed;
    private bool inputFailed;
    private string hintFailure;

    private void CloseSocket(Socket socket)
    {
        lock (gate) { if (!sockets.Remove(socket)) return; }
        socket.Close(0);
    }

    private void LostHint(Exception error)
    {
        lock (gate) {
            if (closed) return; // Close(0) interrupts the outstanding IOControl.
            var native = error as SocketException;
            if (hintFailure == null) hintFailure = native == null ? "-" :
                native.NativeErrorCode.ToString(CultureInfo.InvariantCulture);
            Monitor.PulseAll(gate);
        }
    }

    private void Listen(Socket socket, byte[] wildcard)
    {
        try {
            while (true) {
                lock (gate) { if (closed) return; }
                socket.IOControl(IOControlCode.RoutingInterfaceChange, wildcard, null);
                lock (gate) {
                    if (closed) return;
                    refresh = true;
                    Monitor.PulseAll(gate);
                    // Bound pending work across notifications for every route.
                    while (refresh && !closed) Monitor.Wait(gate);
                }
            }
        } catch (Exception error) { LostHint(error); }
        finally { try { CloseSocket(socket); } catch (Exception error) { LostHint(error); } }
    }

    public void RegisterRouteHint(bool ipv6)
    {
        var address = new IPEndPoint(ipv6 ? IPAddress.IPv6Any : IPAddress.Any, 0);
        var serialized = address.Serialize();
        var bytes = new byte[serialized.Size];
        for (int i = 0; i < bytes.Length; i++) bytes[i] = serialized[i];
        var socket = new Socket(address.AddressFamily, SocketType.Dgram, ProtocolType.Udp);
        lock (gate) { sockets.Add(socket); }
        try {
            if (ipv6) socket.DualMode = false;
            socket.Bind(address);
            var reader = new Thread(new ThreadStart(delegate { Listen(socket, bytes); }));
            reader.IsBackground = true;
            reader.Start();
        } catch { CloseSocket(socket); throw; }
    }

    public void WatchInput()
    {
        bool queued = ThreadPool.QueueUserWorkItem(delegate {
            try { while (Console.In.Read() != -1) {} }
            catch (Exception) { lock (gate) { inputFailed = true; } }
            finally { lock (gate) { closed = true; Monitor.PulseAll(gate); } }
        });
        if (!queued) throw new InvalidOperationException("Unable to watch diagnostic control pipe");
    }

    public string Wait()
    {
        lock (gate) {
            // An interface metric edit can leave route rows and both native
            // sockets silent. Reconcile the active /0 rows after five idle seconds.
            if (!closed && !refresh && hintFailure == null) Monitor.Wait(gate, 5000);
            while (!closed && refresh && hintFailure == null) {
                long remaining = 250 - (clock.ElapsedMilliseconds - lastWakeMs);
                if (remaining <= 0) break;
                Monitor.Wait(gate, (int)remaining);
            }
            if (closed) return inputFailed ? "ERROR\t-" : "CLOSED";
            lastWakeMs = clock.ElapsedMilliseconds;
            refresh = false;
            Monitor.PulseAll(gate);
            if (hintFailure != null) {
                string gap = "GAP\t" + hintFailure;
                hintFailure = null;
                return gap;
            }
            return "REFRESH";
        }
    }

    public void Dispose()
    {
        Socket[] active;
        lock (gate) { closed = true; Monitor.PulseAll(gate); active = sockets.ToArray(); }
        Exception failure = null;
        foreach (var socket in active)
            try { CloseSocket(socket); } catch (Exception error) { failure = error; }
        if (failure != null) throw new InvalidOperationException("Closing route sockets failed", failure);
    }
}
'@
        foreach ($module in 'NetTCPIP', 'NetAdapter') {
            Import-Module (Join-Path $PSHOME "Modules\$module\$module.psd1")
        }
        $watch = [OpenNetRouteWake]::new()
        $hintFailures = @()
        foreach ($ipv6 in $false,$true) {
            try { $watch.RegisterRouteHint($ipv6) } catch { $hintFailures += Get-OpenNetErrorCode $_.Exception }
        }
        $watch.WatchInput()
        [Console]::WriteLine('READY')
        foreach ($code in $hintFailures) { [Console]::WriteLine("GAP`t$code") }
        Write-OpenNetRouteObservations $watch
    } catch {
        [Console]::WriteLine("ERROR`t$(Get-OpenNetErrorCode $_.Exception)")
    } finally {
        if ($null -ne $watch) {
            try { $watch.Dispose() } catch { [Console]::WriteLine("ERROR`t$(Get-OpenNetErrorCode $_.Exception)") }
        }
    }
}

if ($MyInvocation.InvocationName -ne '.') { Start-OpenNetRouteWatcher }
