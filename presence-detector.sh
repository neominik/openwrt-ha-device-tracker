#!/bin/sh
# presence-detector.sh — minimal Wi-Fi presence tracker for Home Assistant via MQTT.
# Shell replacement for presence-detector.py using only OpenWrt built-ins
# (ubus, jsonfilter, logger) plus mosquitto-client-nossl (~75 KB vs ~4 MB Python).
#
# Usage: presence-detector.sh [config-file]      run the tracker (default mode)
#        presence-detector.sh forget <mac>       clear a device's retained MQTT
#                                                messages so HA deletes the entity
#                                                (run after removing it from filter)
#
# Reads the same settings file as the Python version, but only supports
# allowlist mode (filter_is_denylist: false). Robustness model: all publishes
# are retained + a periodic full sync, so broker/HA restarts and missed events
# self-heal without any state tracking or ack queue.

CMD=""
if [ "$1" = "forget" ]; then
    CMD=forget
    MAC=$2
    shift 2
fi

CFG=${1:-/etc/config/presence-detector.settings.json}

cfg() { jsonfilter -i "$CFG" -e "@$1" 2>/dev/null; }

MQTT_HOST=$(cfg .mqtt_host)
MQTT_PORT=$(cfg .mqtt_port); MQTT_PORT=${MQTT_PORT:-1883}
MQTT_USER=$(cfg .mqtt_username)
MQTT_PASS=$(cfg .mqtt_password)
LOC_HOME=$(cfg .location); LOC_HOME=${LOC_HOME:-home}
LOC_AWAY=$(cfg .away); LOC_AWAY=${LOC_AWAY:-not_home}
SYNC=$(cfg .fallback_sync_interval); [ "${SYNC:-0}" -gt 0 ] || SYNC=300
# tr also folds newlines to spaces so tracked()'s case pattern can match
MACS=$(jsonfilter -i "$CFG" -e '@.filter[*]' | tr 'A-F\n' 'a-f ')

log() { logger -t presence-detector -p "daemon.${2:-info}" "$1"; }

pub() { # topic payload (empty payload clears the retained message)
    if [ -n "$2" ]; then set -- -t "$1" -m "$2"; else set -- -t "$1" -n; fi
    mosquitto_pub -h "$MQTT_HOST" -p "$MQTT_PORT" -u "$MQTT_USER" -P "$MQTT_PASS" \
        -q 1 -r "$@" 2>/dev/null
}

if [ "$CMD" = "forget" ]; then
    [ -n "$MAC" ] || { echo "usage: $0 forget <mac> [config-file]" >&2; exit 1; }
    _slug=$(echo "$MAC" | tr 'A-F:' 'a-f_')
    pub "homeassistant/device_tracker/$_slug/config" "" \
        && pub "homeassistant/device_tracker/$_slug/state" "" \
        || { echo "publish failed (broker unreachable?)" >&2; exit 1; }
    echo "cleared retained config and state for $MAC"
    exit 0
fi

[ "$(cfg .filter_is_denylist)" = "false" ] || {
    log "shell version supports allowlist mode only; set filter_is_denylist to false and list MACs in filter" err
    exit 1
}

# At boot this service can start before hostapd registers on ubus; without
# this retry, auto-detection would silently settle on zero interfaces.
IFACES=$(cfg '.interfaces[*]')
while [ -z "$IFACES" ]; do
    IFACES=$(ubus list 'hostapd.*')
    [ -n "$IFACES" ] && break
    log "no hostapd interfaces on ubus yet, retrying in 5s" warn
    sleep 5
done

tracked() { case " $MACS " in *" $1 "*) return 0 ;; *) return 1 ;; esac; }

connected() { # mac -> success if associated on any interface
    for _i in $IFACES; do
        ubus call "$_i" get_clients 2>/dev/null | grep -qF "\"$1\"" && return 0
    done
    return 1
}

publish_state() { # mac state
    _slug=$(echo "$1" | tr ':' '_')
    [ "$2" = "$LOC_HOME" ] && _zones="[\"zone.$LOC_HOME\"]" || _zones="[]"
    pub "homeassistant/device_tracker/$_slug/state" \
        "{\"state\": \"$2\", \"in_zones\": $_zones}" \
        || log "publish failed for $1 (broker offline?), next sync will retry" warn
}

register() { # mac — retained discovery config, republished every sync (idempotent)
    _slug=$(echo "$1" | tr ':' '_')
    _name=$(cfg ".params['$1'].name"); _name=${_name:-$1}
    _icon=$(cfg ".params['$1'].icon")
    [ -n "$_icon" ] && _icon="\"icon\": \"$_icon\"," || _icon=""
    _topic="homeassistant/device_tracker/$_slug/state"
    pub "homeassistant/device_tracker/$_slug/config" "{
        \"state_topic\": \"$_topic\",
        \"json_attributes_topic\": \"$_topic\",
        \"value_template\": \"{{ value_json['state'] }}\",
        \"name\": \"$_name\", $_icon
        \"payload_home\": \"$LOC_HOME\",
        \"payload_not_home\": \"$LOC_AWAY\",
        \"source_type\": \"router\",
        \"unique_id\": \"$_slug\",
        \"device\": {\"connections\": [[\"mac\", \"$1\"]], \"name\": \"$_name\"}
    }"
}

sync_all() {
    for _mac in $MACS; do
        register "$_mac"
        if connected "$_mac"; then
            publish_state "$_mac" "$LOC_HOME"
        else
            publish_state "$_mac" "$LOC_AWAY"
        fi
    done
}

watch() { # interface — react to assoc/disassoc events in realtime
    while :; do
        ubus subscribe "$1" | while read -r _line; do
            _mac=$(echo "$_line" | jsonfilter -e '@.assoc.address' 2>/dev/null | tr 'A-F' 'a-f')
            if [ -n "$_mac" ]; then
                tracked "$_mac" && { log "$_mac joined $1"; publish_state "$_mac" "$LOC_HOME"; }
                continue
            fi
            _mac=$(echo "$_line" | jsonfilter -e '@.disassoc.address' 2>/dev/null | tr 'A-F' 'a-f')
            if [ -n "$_mac" ] && tracked "$_mac"; then
                sleep 2 # ponytail: crude roam debounce; track per-interface state if home/away flicker ever matters
                if ! connected "$_mac"; then
                    log "$_mac left $1"
                    publish_state "$_mac" "$LOC_AWAY"
                fi
            fi
        done
        log "ubus subscribe $1 exited, retrying in 5s" warn
        sleep 5
    done
}

# Kill only pids we know about, never a process group. Under procd this
# shell can share init's process group, where a group-wide signal takes
# down the whole router.
cleanup() {
    kill $WATCHERS 2>/dev/null
    for _i in $IFACES; do
        kill $(pgrep -f "ubus subscribe $_i") 2>/dev/null
    done
    exit 0
}
trap cleanup TERM INT

log "starting: tracking [$(echo $MACS)] on [$(echo $IFACES)]"
WATCHERS=""
for _i in $IFACES; do
    watch "$_i" &
    WATCHERS="$WATCHERS $!"
done

while :; do
    sync_all
    sleep "$SYNC" &
    wait $!
done
