-- WKI (WOS Kernel Interconnect) Protocol Dissector for Wireshark
-- Based on WKI Architecture Specification V1
--
-- Installation:
--   Linux:   ~/.local/lib/wireshark/plugins/wki.lua
--   macOS:   ~/Library/Application Support/Wireshark/plugins/wki.lua
--   Windows: %APPDATA%\Wireshark\plugins\wki.lua
--
-- Or load via: wireshark -X lua_script:wki.lua

-- Protocol constants
local WKI_ETHERTYPE = 0x88B7
local WKI_VERSION = 1
local WKI_HELLO_MAGIC = 0x574B4900  -- "WKI\0"
local WKI_HEADER_SIZE = 32

-- Create the protocol
local wki_proto = Proto("wki", "WOS Kernel Interconnect")

-- Header flag definitions
local header_flags = {
    [0x08] = "ACK_PRESENT",
    [0x04] = "PRIORITY",
    [0x02] = "FRAGMENT",
    [0x01] = "RESERVED",
}

-- Message type definitions
local msg_types = {
    -- Control plane (channel 0)
    [0x01] = "HELLO",
    [0x02] = "HELLO_ACK",
    [0x03] = "HEARTBEAT",
    [0x04] = "HEARTBEAT_ACK",
    [0x05] = "LSA",
    [0x06] = "LSA_ACK",
    [0x07] = "FENCE_NOTIFY",
    [0x08] = "RECONCILE_REQ",
    [0x09] = "RECONCILE_ACK",
    [0x0A] = "RESOURCE_ADVERT",
    [0x0B] = "RESOURCE_WITHDRAW",

    -- Zone management (channel 1)
    [0x20] = "ZONE_CREATE_REQ",
    [0x21] = "ZONE_CREATE_ACK",
    [0x22] = "ZONE_DESTROY",
    [0x23] = "ZONE_NOTIFY_PRE",
    [0x24] = "ZONE_NOTIFY_POST",
    [0x25] = "ZONE_READ_REQ",
    [0x26] = "ZONE_READ_RESP",
    [0x27] = "ZONE_WRITE_REQ",
    [0x28] = "ZONE_WRITE_ACK",
    [0x29] = "ZONE_NOTIFY_PRE_ACK",
    [0x2A] = "ZONE_NOTIFY_POST_ACK",

    -- Event bus (channel 2)
    [0x30] = "EVENT_SUBSCRIBE",
    [0x31] = "EVENT_UNSUBSCRIBE",
    [0x32] = "EVENT_PUBLISH",
    [0x33] = "EVENT_ACK",

    -- Resource operations (channel 3 + dynamic)
    [0x40] = "DEV_ATTACH_REQ",
    [0x41] = "DEV_ATTACH_ACK",
    [0x42] = "DEV_DETACH",
    [0x43] = "DEV_OP_REQ",
    [0x44] = "DEV_OP_RESP",
    [0x45] = "DEV_IRQ_FWD",
    [0x46] = "CHANNEL_OPEN",
    [0x47] = "CHANNEL_OPEN_ACK",
    [0x48] = "CHANNEL_CLOSE",

    -- Compute (uses RESOURCE channel)
    [0x50] = "TASK_SUBMIT",
    [0x51] = "TASK_ACCEPT",
    [0x52] = "TASK_REJECT",
    [0x53] = "TASK_COMPLETE",
    [0x54] = "TASK_CANCEL",
    [0x55] = "LOAD_REPORT",
}

-- Well-known channel IDs
local channel_names = {
    [0] = "CONTROL",
    [1] = "ZONE_MGMT",
    [2] = "EVENT_BUS",
    [3] = "RESOURCE",
}

-- Resource types
local resource_types = {
    [1] = "BLOCK",
    [2] = "CHAR",
    [3] = "NET",
    [4] = "VFS",
    [5] = "COMPUTE",
    [6] = "CUSTOM",
}

-- Device operation IDs
local op_ids = {
    [0x0100] = "BLOCK_READ",
    [0x0101] = "BLOCK_WRITE",
    [0x0102] = "BLOCK_FLUSH",
    [0x0200] = "CHAR_OPEN",
    [0x0201] = "CHAR_CLOSE",
    [0x0202] = "CHAR_READ",
    [0x0203] = "CHAR_WRITE",
    [0x0300] = "NET_XMIT",
    [0x0301] = "NET_SET_MAC",
    [0x0302] = "NET_RX_NOTIFY",
    [0x0303] = "NET_GET_STATS",
    [0x0400] = "VFS_OPEN",
    [0x0401] = "VFS_READ",
    [0x0402] = "VFS_WRITE",
    [0x0403] = "VFS_CLOSE",
    [0x0404] = "VFS_READDIR",
    [0x0405] = "VFS_STAT",
    [0x0406] = "VFS_MKDIR",
    [0x0407] = "VFS_READLINK",
    [0x0408] = "VFS_SYMLINK",
}

-- Event classes
local event_classes = {
    [0x0001] = "SYSTEM",
    [0x0002] = "MEMORY",
    [0x0003] = "SCHEDULER",
    [0x0004] = "DEVICE",
    [0x0005] = "STORAGE",
    [0x0006] = "ZONE",
    [0x8000] = "CUSTOM",
}

-- Capability flags
local capabilities = {
    [0x0001] = "RDMA_SUPPORT",
    [0x0002] = "ZONE_SUPPORT",
}

-- Zone create status
local zone_create_status = {
    [0] = "ACCEPTED",
    [1] = "REJECTED_NO_MEM",
    [2] = "REJECTED_POLICY",
}

-- Device attach status
local dev_attach_status = {
    [0] = "OK",
    [1] = "NOT_FOUND",
    [2] = "NOT_REMOTABLE",
    [3] = "BUSY",
    [4] = "NO_PASSTHROUGH",
}

-- Task reject reasons
local task_reject_reasons = {
    [0] = "ACCEPTED",
    [1] = "OVERLOADED",
    [2] = "NO_MEM",
    [3] = "BINARY_NOT_FOUND",
    [4] = "FETCH_FAILED",
}

-- Priority classes
local priority_classes = {
    [0] = "LATENCY",
    [1] = "THROUGHPUT",
}

-- Attach modes
local attach_modes = {
    [0] = "PROXY",
    [1] = "PASSTHROUGH",
}

-- Zone notify modes
local zone_notify_modes = {
    [0] = "NONE",
    [1] = "PRE_ONLY",
    [2] = "POST_ONLY",
    [3] = "PRE_AND_POST",
}

-- Zone type hints
local zone_type_hints = {
    [0] = "BUFFER",
    [1] = "MSG_QUEUE",
    [2] = "LOCK",
    [3] = "CUSTOM",
}

-- Task delivery modes
local task_delivery_modes = {
    [0] = "INLINE",
    [1] = "VFS_REF",
    [2] = "RESOURCE_REF",
}

-- Helper: get channel name
local function get_channel_name(id)
    if channel_names[id] then
        return channel_names[id]
    elseif id >= 16 then
        return string.format("DYNAMIC_%d", id - 16)
    else
        return string.format("RESERVED_%d", id)
    end
end

-- Helper: build flags string
local function build_flags_string(flags)
    local parts = {}
    if bit.band(flags, 0x08) ~= 0 then table.insert(parts, "ACK_PRESENT") end
    if bit.band(flags, 0x04) ~= 0 then table.insert(parts, "PRIORITY") end
    if bit.band(flags, 0x02) ~= 0 then table.insert(parts, "FRAGMENT") end
    if bit.band(flags, 0x01) ~= 0 then table.insert(parts, "RESERVED") end
    if #parts == 0 then return "none" end
    return table.concat(parts, ", ")
end

-- Helper: build capabilities string
local function build_caps_string(caps)
    local parts = {}
    if bit.band(caps, 0x0001) ~= 0 then table.insert(parts, "RDMA") end
    if bit.band(caps, 0x0002) ~= 0 then table.insert(parts, "ZONE") end
    if #parts == 0 then return "none" end
    return table.concat(parts, ", ")
end

-- Helper: format node ID
local function format_node_id(id)
    if id == 0x0000 then return "INVALID (0x0000)" end
    if id == 0xFFFF then return "BROADCAST (0xFFFF)" end
    return string.format("0x%04X", id)
end

-- Protocol fields - Header
local pf_version_flags = ProtoField.uint8("wki.version_flags", "Version/Flags", base.HEX)
local pf_version = ProtoField.uint8("wki.version", "Version", base.DEC, nil, 0xF0)
local pf_flags = ProtoField.uint8("wki.flags", "Flags", base.HEX, nil, 0x0F)
local pf_flag_ack = ProtoField.bool("wki.flags.ack", "ACK Present", 8, nil, 0x08)
local pf_flag_priority = ProtoField.bool("wki.flags.priority", "Priority", 8, nil, 0x04)
local pf_flag_fragment = ProtoField.bool("wki.flags.fragment", "Fragment", 8, nil, 0x02)
local pf_flag_reserved = ProtoField.bool("wki.flags.reserved", "Reserved", 8, nil, 0x01)
local pf_msg_type = ProtoField.uint8("wki.msg_type", "Message Type", base.HEX, msg_types)
local pf_src_node = ProtoField.uint16("wki.src_node", "Source Node", base.HEX)
local pf_dst_node = ProtoField.uint16("wki.dst_node", "Destination Node", base.HEX)
local pf_channel_id = ProtoField.uint16("wki.channel", "Channel ID", base.DEC)
local pf_seq_num = ProtoField.uint32("wki.seq", "Sequence Number", base.DEC)
local pf_ack_num = ProtoField.uint32("wki.ack", "ACK Number", base.DEC)
local pf_payload_len = ProtoField.uint16("wki.payload_len", "Payload Length", base.DEC)
local pf_credits = ProtoField.uint8("wki.credits", "Credits", base.DEC)
local pf_hop_ttl = ProtoField.uint8("wki.ttl", "Hop TTL", base.DEC)
local pf_src_port = ProtoField.uint16("wki.src_port", "Source Port", base.DEC)
local pf_dst_port = ProtoField.uint16("wki.dst_port", "Destination Port", base.DEC)
local pf_checksum = ProtoField.uint32("wki.checksum", "Checksum", base.HEX)
local pf_reserved = ProtoField.uint32("wki.reserved", "Reserved", base.HEX)

-- Protocol fields - HELLO payload
local pf_hello_magic = ProtoField.uint32("wki.hello.magic", "Magic", base.HEX)
local pf_hello_proto_ver = ProtoField.uint16("wki.hello.proto_version", "Protocol Version", base.DEC)
local pf_hello_node_id = ProtoField.uint16("wki.hello.node_id", "Node ID", base.HEX)
local pf_hello_mac = ProtoField.ether("wki.hello.mac", "MAC Address")
local pf_hello_caps = ProtoField.uint16("wki.hello.capabilities", "Capabilities", base.HEX)
local pf_hello_hb_interval = ProtoField.uint16("wki.hello.heartbeat_interval", "Heartbeat Interval (ms)", base.DEC)
local pf_hello_max_channels = ProtoField.uint16("wki.hello.max_channels", "Max Channels", base.DEC)
local pf_hello_rdma_zones = ProtoField.uint32("wki.hello.rdma_zones", "RDMA Zone Bitmap", base.HEX)

-- Protocol fields - HEARTBEAT payload
local pf_hb_timestamp = ProtoField.uint64("wki.heartbeat.timestamp", "Timestamp (ns)", base.DEC)
local pf_hb_load = ProtoField.uint16("wki.heartbeat.load", "Sender Load", base.DEC)
local pf_hb_mem_free = ProtoField.uint16("wki.heartbeat.mem_free", "Free Memory (256-page units)", base.DEC)

-- Protocol fields - LSA payload
local pf_lsa_origin = ProtoField.uint16("wki.lsa.origin", "Origin Node", base.HEX)
local pf_lsa_seq = ProtoField.uint32("wki.lsa.seq", "LSA Sequence", base.DEC)
local pf_lsa_num_neighbors = ProtoField.uint16("wki.lsa.num_neighbors", "Neighbor Count", base.DEC)
local pf_lsa_rdma_zones = ProtoField.uint32("wki.lsa.rdma_zones", "RDMA Zone Bitmap", base.HEX)
local pf_lsa_neighbor_id = ProtoField.uint16("wki.lsa.neighbor.id", "Neighbor ID", base.HEX)
local pf_lsa_neighbor_cost = ProtoField.uint16("wki.lsa.neighbor.cost", "Link Cost", base.DEC)
local pf_lsa_neighbor_mtu = ProtoField.uint16("wki.lsa.neighbor.mtu", "Transport MTU", base.DEC)

-- Protocol fields - FENCE_NOTIFY payload
local pf_fence_fenced = ProtoField.uint16("wki.fence.fenced_node", "Fenced Node", base.HEX)
local pf_fence_fencing = ProtoField.uint16("wki.fence.fencing_node", "Fencing Node", base.HEX)
local pf_fence_reason = ProtoField.uint32("wki.fence.reason", "Reason", base.DEC)

-- Protocol fields - RESOURCE_ADVERT payload
local pf_res_node = ProtoField.uint16("wki.resource.node_id", "Owner Node", base.HEX)
local pf_res_type = ProtoField.uint16("wki.resource.type", "Resource Type", base.DEC, resource_types)
local pf_res_id = ProtoField.uint32("wki.resource.id", "Resource ID", base.DEC)
local pf_res_flags = ProtoField.uint8("wki.resource.flags", "Flags", base.HEX)
local pf_res_name_len = ProtoField.uint8("wki.resource.name_len", "Name Length", base.DEC)
local pf_res_name = ProtoField.string("wki.resource.name", "Resource Name")

-- Protocol fields - ZONE_CREATE_REQ payload
local pf_zone_id = ProtoField.uint32("wki.zone.id", "Zone ID", base.HEX)
local pf_zone_size = ProtoField.uint32("wki.zone.size", "Size (bytes)", base.DEC)
local pf_zone_access = ProtoField.uint8("wki.zone.access_policy", "Access Policy", base.HEX)
local pf_zone_notify_mode = ProtoField.uint8("wki.zone.notify_mode", "Notify Mode", base.DEC, zone_notify_modes)
local pf_zone_type = ProtoField.uint8("wki.zone.type_hint", "Type Hint", base.DEC, zone_type_hints)

-- Protocol fields - ZONE_CREATE_ACK payload
local pf_zone_status = ProtoField.uint8("wki.zone.status", "Status", base.DEC, zone_create_status)
local pf_zone_phys_addr = ProtoField.uint64("wki.zone.phys_addr", "Physical Address", base.HEX)
local pf_zone_rkey = ProtoField.uint32("wki.zone.rkey", "RDMA Key", base.HEX)

-- Protocol fields - ZONE_NOTIFY payload
local pf_zone_notify_offset = ProtoField.uint32("wki.zone.notify.offset", "Offset", base.DEC)
local pf_zone_notify_length = ProtoField.uint32("wki.zone.notify.length", "Length", base.DEC)
local pf_zone_notify_op = ProtoField.uint8("wki.zone.notify.op_type", "Operation", base.DEC)

-- Protocol fields - EVENT payload
local pf_event_class = ProtoField.uint16("wki.event.class", "Event Class", base.HEX, event_classes)
local pf_event_id = ProtoField.uint16("wki.event.id", "Event ID", base.HEX)
local pf_event_origin = ProtoField.uint16("wki.event.origin", "Origin Node", base.HEX)
local pf_event_data_len = ProtoField.uint16("wki.event.data_len", "Data Length", base.DEC)
local pf_event_delivery = ProtoField.uint8("wki.event.delivery", "Delivery Mode", base.DEC)

-- Protocol fields - DEV_ATTACH_REQ payload
local pf_dev_target = ProtoField.uint16("wki.dev.target_node", "Target Node", base.HEX)
local pf_dev_res_type = ProtoField.uint16("wki.dev.resource_type", "Resource Type", base.DEC, resource_types)
local pf_dev_res_id = ProtoField.uint32("wki.dev.resource_id", "Resource ID", base.DEC)
local pf_dev_attach_mode = ProtoField.uint8("wki.dev.attach_mode", "Attach Mode", base.DEC, attach_modes)
local pf_dev_req_channel = ProtoField.uint16("wki.dev.requested_channel", "Requested Channel", base.DEC)

-- Protocol fields - DEV_ATTACH_ACK payload
local pf_dev_status = ProtoField.uint8("wki.dev.status", "Status", base.DEC, dev_attach_status)
local pf_dev_assigned_ch = ProtoField.uint16("wki.dev.assigned_channel", "Assigned Channel", base.DEC)
local pf_dev_max_op_size = ProtoField.uint16("wki.dev.max_op_size", "Max Op Size", base.DEC)

-- Protocol fields - DEV_OP payload
local pf_dev_op_id = ProtoField.uint16("wki.dev.op_id", "Operation ID", base.HEX, op_ids)
local pf_dev_op_data_len = ProtoField.uint16("wki.dev.data_len", "Data Length", base.DEC)
local pf_dev_op_status = ProtoField.int16("wki.dev.op_status", "Status", base.DEC)

-- Protocol fields - DEV_IRQ_FWD payload
local pf_irq_device = ProtoField.uint16("wki.irq.device_id", "Device ID", base.DEC)
local pf_irq_vector = ProtoField.uint16("wki.irq.vector", "IRQ Vector", base.DEC)
local pf_irq_status = ProtoField.uint32("wki.irq.status", "IRQ Status", base.HEX)

-- Protocol fields - CHANNEL_OPEN payload
local pf_chan_req_id = ProtoField.uint16("wki.channel.requested_id", "Requested ID", base.DEC)
local pf_chan_priority = ProtoField.uint8("wki.channel.priority", "Priority", base.DEC, priority_classes)
local pf_chan_credits = ProtoField.uint16("wki.channel.initial_credits", "Initial Credits", base.DEC)
local pf_chan_assigned_id = ProtoField.uint16("wki.channel.assigned_id", "Assigned ID", base.DEC)
local pf_chan_status = ProtoField.uint8("wki.channel.status", "Status", base.DEC)

-- Protocol fields - TASK payload
local pf_task_id = ProtoField.uint32("wki.task.id", "Task ID", base.DEC)
local pf_task_delivery = ProtoField.uint8("wki.task.delivery_mode", "Delivery Mode", base.DEC, task_delivery_modes)
local pf_task_args_len = ProtoField.uint16("wki.task.args_len", "Args Length", base.DEC)
local pf_task_status = ProtoField.uint8("wki.task.status", "Status", base.DEC, task_reject_reasons)
local pf_task_remote_pid = ProtoField.uint64("wki.task.remote_pid", "Remote PID", base.DEC)
local pf_task_exit_status = ProtoField.int32("wki.task.exit_status", "Exit Status", base.DEC)
local pf_task_output_len = ProtoField.uint16("wki.task.output_len", "Output Length", base.DEC)

-- Protocol fields - LOAD_REPORT payload
local pf_load_num_cpus = ProtoField.uint16("wki.load.num_cpus", "Number of CPUs", base.DEC)
local pf_load_runnable = ProtoField.uint16("wki.load.runnable_tasks", "Runnable Tasks", base.DEC)
local pf_load_avg = ProtoField.uint16("wki.load.avg_load_pct", "Average Load (0.1%)", base.DEC)
local pf_load_free_mem = ProtoField.uint16("wki.load.free_mem_pages", "Free Memory (256-page units)", base.DEC)

-- Payload data
local pf_payload_data = ProtoField.bytes("wki.payload", "Payload Data")

-- Add all fields to protocol
wki_proto.fields = {
    -- Header
    pf_version_flags, pf_version, pf_flags,
    pf_flag_ack, pf_flag_priority, pf_flag_fragment, pf_flag_reserved,
    pf_msg_type, pf_src_node, pf_dst_node, pf_channel_id,
    pf_seq_num, pf_ack_num, pf_payload_len, pf_credits, pf_hop_ttl,
    pf_src_port, pf_dst_port, pf_checksum, pf_reserved,
    -- HELLO
    pf_hello_magic, pf_hello_proto_ver, pf_hello_node_id, pf_hello_mac,
    pf_hello_caps, pf_hello_hb_interval, pf_hello_max_channels, pf_hello_rdma_zones,
    -- HEARTBEAT
    pf_hb_timestamp, pf_hb_load, pf_hb_mem_free,
    -- LSA
    pf_lsa_origin, pf_lsa_seq, pf_lsa_num_neighbors, pf_lsa_rdma_zones,
    pf_lsa_neighbor_id, pf_lsa_neighbor_cost, pf_lsa_neighbor_mtu,
    -- FENCE
    pf_fence_fenced, pf_fence_fencing, pf_fence_reason,
    -- RESOURCE
    pf_res_node, pf_res_type, pf_res_id, pf_res_flags, pf_res_name_len, pf_res_name,
    -- ZONE
    pf_zone_id, pf_zone_size, pf_zone_access, pf_zone_notify_mode, pf_zone_type,
    pf_zone_status, pf_zone_phys_addr, pf_zone_rkey,
    pf_zone_notify_offset, pf_zone_notify_length, pf_zone_notify_op,
    -- EVENT
    pf_event_class, pf_event_id, pf_event_origin, pf_event_data_len, pf_event_delivery,
    -- DEV
    pf_dev_target, pf_dev_res_type, pf_dev_res_id, pf_dev_attach_mode, pf_dev_req_channel,
    pf_dev_status, pf_dev_assigned_ch, pf_dev_max_op_size,
    pf_dev_op_id, pf_dev_op_data_len, pf_dev_op_status,
    -- IRQ
    pf_irq_device, pf_irq_vector, pf_irq_status,
    -- CHANNEL
    pf_chan_req_id, pf_chan_priority, pf_chan_credits, pf_chan_assigned_id, pf_chan_status,
    -- TASK
    pf_task_id, pf_task_delivery, pf_task_args_len, pf_task_status, pf_task_remote_pid,
    pf_task_exit_status, pf_task_output_len,
    -- LOAD
    pf_load_num_cpus, pf_load_runnable, pf_load_avg, pf_load_free_mem,
    -- Payload
    pf_payload_data,
}

-- Dissector function
function wki_proto.dissector(buffer, pinfo, tree)
    local length = buffer:len()
    if length < WKI_HEADER_SIZE then return end

    pinfo.cols.protocol = wki_proto.name

    -- Parse header fields
    local version_flags = buffer(0, 1):uint()
    local version = bit.rshift(version_flags, 4)
    local flags = bit.band(version_flags, 0x0F)
    local msg_type = buffer(1, 1):uint()
    local src_node = buffer(2, 2):le_uint()
    local dst_node = buffer(4, 2):le_uint()
    local channel_id = buffer(6, 2):le_uint()
    local seq_num = buffer(8, 4):le_uint()
    local ack_num = buffer(12, 4):le_uint()
    local payload_len = buffer(16, 2):le_uint()
    local credits = buffer(18, 1):uint()
    local hop_ttl = buffer(19, 1):uint()

    -- Build info column
    local msg_name = msg_types[msg_type] or string.format("UNKNOWN(0x%02X)", msg_type)
    local channel_name = get_channel_name(channel_id)
    pinfo.cols.info = string.format("%s %s->%s ch=%s seq=%d",
        msg_name,
        format_node_id(src_node),
        format_node_id(dst_node),
        channel_name,
        seq_num)

    -- Add to tree
    local subtree = tree:add(wki_proto, buffer(), "WKI Protocol")

    -- Header subtree
    local hdr_tree = subtree:add(wki_proto, buffer(0, WKI_HEADER_SIZE), "Header")

    local vf_tree = hdr_tree:add(pf_version_flags, buffer(0, 1))
    vf_tree:add(pf_version, buffer(0, 1))
    local flags_tree = vf_tree:add(pf_flags, buffer(0, 1))
    flags_tree:add(pf_flag_ack, buffer(0, 1))
    flags_tree:add(pf_flag_priority, buffer(0, 1))
    flags_tree:add(pf_flag_fragment, buffer(0, 1))
    flags_tree:add(pf_flag_reserved, buffer(0, 1))
    flags_tree:append_text(" (" .. build_flags_string(flags) .. ")")

    hdr_tree:add(pf_msg_type, buffer(1, 1)):append_text(" (" .. msg_name .. ")")
    hdr_tree:add_le(pf_src_node, buffer(2, 2)):append_text(" (" .. format_node_id(src_node) .. ")")
    hdr_tree:add_le(pf_dst_node, buffer(4, 2)):append_text(" (" .. format_node_id(dst_node) .. ")")
    hdr_tree:add_le(pf_channel_id, buffer(6, 2)):append_text(" (" .. channel_name .. ")")
    hdr_tree:add_le(pf_seq_num, buffer(8, 4))

    local ack_item = hdr_tree:add_le(pf_ack_num, buffer(12, 4))
    if bit.band(flags, 0x08) == 0 then
        ack_item:append_text(" (not valid)")
    end

    hdr_tree:add_le(pf_payload_len, buffer(16, 2))
    hdr_tree:add(pf_credits, buffer(18, 1))
    hdr_tree:add(pf_hop_ttl, buffer(19, 1))
    hdr_tree:add_le(pf_src_port, buffer(20, 2))
    hdr_tree:add_le(pf_dst_port, buffer(22, 2))
    hdr_tree:add_le(pf_checksum, buffer(24, 4))
    hdr_tree:add_le(pf_reserved, buffer(28, 4))

    -- Parse payload based on message type
    if payload_len > 0 and length >= WKI_HEADER_SIZE + payload_len then
        local payload_buf = buffer(WKI_HEADER_SIZE, payload_len)
        local payload_tree = subtree:add(wki_proto, payload_buf, "Payload (" .. msg_name .. ")")

        -- HELLO / HELLO_ACK
        if msg_type == 0x01 or msg_type == 0x02 then
            if payload_len >= 32 then
                local magic = payload_buf(0, 4):le_uint()
                payload_tree:add_le(pf_hello_magic, payload_buf(0, 4))
                    :append_text(magic == WKI_HELLO_MAGIC and " (valid)" or " (INVALID!)")
                payload_tree:add_le(pf_hello_proto_ver, payload_buf(4, 2))
                payload_tree:add_le(pf_hello_node_id, payload_buf(6, 2))
                payload_tree:add(pf_hello_mac, payload_buf(8, 6))
                local caps = payload_buf(14, 2):le_uint()
                payload_tree:add_le(pf_hello_caps, payload_buf(14, 2))
                    :append_text(" (" .. build_caps_string(caps) .. ")")
                payload_tree:add_le(pf_hello_hb_interval, payload_buf(16, 2))
                payload_tree:add_le(pf_hello_max_channels, payload_buf(18, 2))
                payload_tree:add_le(pf_hello_rdma_zones, payload_buf(20, 4))
            end

        -- HEARTBEAT / HEARTBEAT_ACK
        elseif msg_type == 0x03 or msg_type == 0x04 then
            if payload_len >= 16 then
                payload_tree:add_le(pf_hb_timestamp, payload_buf(0, 8))
                payload_tree:add_le(pf_hb_load, payload_buf(8, 2))
                payload_tree:add_le(pf_hb_mem_free, payload_buf(10, 2))
            end

        -- LSA
        elseif msg_type == 0x05 then
            if payload_len >= 10 then
                payload_tree:add_le(pf_lsa_origin, payload_buf(0, 2))
                payload_tree:add_le(pf_lsa_seq, payload_buf(2, 4))
                local num_neighbors = payload_buf(6, 2):le_uint()
                payload_tree:add_le(pf_lsa_num_neighbors, payload_buf(6, 2))
                payload_tree:add_le(pf_lsa_rdma_zones, payload_buf(8, 4))

                -- Parse neighbor entries
                local offset = 12
                for i = 1, num_neighbors do
                    if offset + 6 <= payload_len then
                        local nbr_tree = payload_tree:add(wki_proto, payload_buf(offset, 6),
                            string.format("Neighbor %d", i))
                        nbr_tree:add_le(pf_lsa_neighbor_id, payload_buf(offset, 2))
                        nbr_tree:add_le(pf_lsa_neighbor_cost, payload_buf(offset + 2, 2))
                        nbr_tree:add_le(pf_lsa_neighbor_mtu, payload_buf(offset + 4, 2))
                        offset = offset + 6
                    end
                end
            end

        -- FENCE_NOTIFY
        elseif msg_type == 0x07 then
            if payload_len >= 8 then
                payload_tree:add_le(pf_fence_fenced, payload_buf(0, 2))
                payload_tree:add_le(pf_fence_fencing, payload_buf(2, 2))
                local reason = payload_buf(4, 4):le_uint()
                payload_tree:add_le(pf_fence_reason, payload_buf(4, 4))
                    :append_text(reason == 0 and " (heartbeat timeout)" or " (manual)")
            end

        -- RESOURCE_ADVERT / RESOURCE_WITHDRAW
        elseif msg_type == 0x0A or msg_type == 0x0B then
            if payload_len >= 10 then
                payload_tree:add_le(pf_res_node, payload_buf(0, 2))
                payload_tree:add_le(pf_res_type, payload_buf(2, 2))
                payload_tree:add_le(pf_res_id, payload_buf(4, 4))
                payload_tree:add(pf_res_flags, payload_buf(8, 1))
                local name_len = payload_buf(9, 1):uint()
                payload_tree:add(pf_res_name_len, payload_buf(9, 1))
                if name_len > 0 and payload_len >= 10 + name_len then
                    payload_tree:add(pf_res_name, payload_buf(10, name_len))
                end
            end

        -- ZONE_CREATE_REQ
        elseif msg_type == 0x20 then
            if payload_len >= 16 then
                payload_tree:add_le(pf_zone_id, payload_buf(0, 4))
                payload_tree:add_le(pf_zone_size, payload_buf(4, 4))
                payload_tree:add(pf_zone_access, payload_buf(8, 1))
                payload_tree:add(pf_zone_notify_mode, payload_buf(9, 1))
                payload_tree:add(pf_zone_type, payload_buf(10, 1))
            end

        -- ZONE_CREATE_ACK
        elseif msg_type == 0x21 then
            if payload_len >= 24 then
                payload_tree:add_le(pf_zone_id, payload_buf(0, 4))
                payload_tree:add(pf_zone_status, payload_buf(4, 1))
                payload_tree:add_le(pf_zone_phys_addr, payload_buf(8, 8))
                payload_tree:add_le(pf_zone_rkey, payload_buf(16, 4))
            end

        -- ZONE_DESTROY
        elseif msg_type == 0x22 then
            if payload_len >= 4 then
                payload_tree:add_le(pf_zone_id, payload_buf(0, 4))
            end

        -- ZONE_NOTIFY_PRE / ZONE_NOTIFY_POST
        elseif msg_type == 0x23 or msg_type == 0x24 then
            if payload_len >= 16 then
                payload_tree:add_le(pf_zone_id, payload_buf(0, 4))
                payload_tree:add_le(pf_zone_notify_offset, payload_buf(4, 4))
                payload_tree:add_le(pf_zone_notify_length, payload_buf(8, 4))
                local op = payload_buf(12, 1):uint()
                payload_tree:add(pf_zone_notify_op, payload_buf(12, 1))
                    :append_text(op == 0 and " (READ)" or " (WRITE)")
            end

        -- EVENT_SUBSCRIBE / EVENT_UNSUBSCRIBE
        elseif msg_type == 0x30 or msg_type == 0x31 then
            if payload_len >= 8 then
                payload_tree:add_le(pf_event_class, payload_buf(0, 2))
                payload_tree:add_le(pf_event_id, payload_buf(2, 2))
                payload_tree:add(pf_event_delivery, payload_buf(4, 1))
            end

        -- EVENT_PUBLISH
        elseif msg_type == 0x32 then
            if payload_len >= 8 then
                payload_tree:add_le(pf_event_class, payload_buf(0, 2))
                payload_tree:add_le(pf_event_id, payload_buf(2, 2))
                payload_tree:add_le(pf_event_origin, payload_buf(4, 2))
                local data_len = payload_buf(6, 2):le_uint()
                payload_tree:add_le(pf_event_data_len, payload_buf(6, 2))
                if data_len > 0 and payload_len >= 8 + data_len then
                    payload_tree:add(pf_payload_data, payload_buf(8, data_len))
                end
            end

        -- DEV_ATTACH_REQ
        elseif msg_type == 0x40 then
            if payload_len >= 12 then
                payload_tree:add_le(pf_dev_target, payload_buf(0, 2))
                payload_tree:add_le(pf_dev_res_type, payload_buf(2, 2))
                payload_tree:add_le(pf_dev_res_id, payload_buf(4, 4))
                payload_tree:add(pf_dev_attach_mode, payload_buf(8, 1))
                payload_tree:add_le(pf_dev_req_channel, payload_buf(10, 2))
            end

        -- DEV_ATTACH_ACK
        elseif msg_type == 0x41 then
            if payload_len >= 8 then
                payload_tree:add(pf_dev_status, payload_buf(0, 1))
                payload_tree:add_le(pf_dev_assigned_ch, payload_buf(2, 2))
                payload_tree:add_le(pf_dev_max_op_size, payload_buf(4, 2))
            end

        -- DEV_DETACH
        elseif msg_type == 0x42 then
            if payload_len >= 8 then
                payload_tree:add_le(pf_dev_target, payload_buf(0, 2))
                payload_tree:add_le(pf_dev_res_type, payload_buf(2, 2))
                payload_tree:add_le(pf_dev_res_id, payload_buf(4, 4))
            end

        -- DEV_OP_REQ
        elseif msg_type == 0x43 then
            if payload_len >= 4 then
                local op_id = payload_buf(0, 2):le_uint()
                local op_name = op_ids[op_id] or string.format("0x%04X", op_id)
                payload_tree:add_le(pf_dev_op_id, payload_buf(0, 2)):append_text(" (" .. op_name .. ")")
                local data_len = payload_buf(2, 2):le_uint()
                payload_tree:add_le(pf_dev_op_data_len, payload_buf(2, 2))
                if data_len > 0 and payload_len >= 4 + data_len then
                    payload_tree:add(pf_payload_data, payload_buf(4, data_len))
                end
            end

        -- DEV_OP_RESP
        elseif msg_type == 0x44 then
            if payload_len >= 8 then
                local op_id = payload_buf(0, 2):le_uint()
                local op_name = op_ids[op_id] or string.format("0x%04X", op_id)
                payload_tree:add_le(pf_dev_op_id, payload_buf(0, 2)):append_text(" (" .. op_name .. ")")
                payload_tree:add_le(pf_dev_op_status, payload_buf(2, 2))
                local data_len = payload_buf(4, 2):le_uint()
                payload_tree:add_le(pf_dev_op_data_len, payload_buf(4, 2))
                if data_len > 0 and payload_len >= 8 + data_len then
                    payload_tree:add(pf_payload_data, payload_buf(8, data_len))
                end
            end

        -- DEV_IRQ_FWD
        elseif msg_type == 0x45 then
            if payload_len >= 8 then
                payload_tree:add_le(pf_irq_device, payload_buf(0, 2))
                payload_tree:add_le(pf_irq_vector, payload_buf(2, 2))
                payload_tree:add_le(pf_irq_status, payload_buf(4, 4))
            end

        -- CHANNEL_OPEN
        elseif msg_type == 0x46 then
            if payload_len >= 8 then
                payload_tree:add_le(pf_chan_req_id, payload_buf(0, 2))
                payload_tree:add(pf_chan_priority, payload_buf(2, 1))
                payload_tree:add_le(pf_chan_credits, payload_buf(4, 2))
            end

        -- CHANNEL_OPEN_ACK
        elseif msg_type == 0x47 then
            if payload_len >= 8 then
                payload_tree:add_le(pf_chan_assigned_id, payload_buf(0, 2))
                payload_tree:add(pf_chan_status, payload_buf(2, 1))
                payload_tree:add_le(pf_chan_credits, payload_buf(4, 2))
            end

        -- CHANNEL_CLOSE
        elseif msg_type == 0x48 then
            if payload_len >= 4 then
                payload_tree:add_le(pf_channel_id, payload_buf(0, 2))
            end

        -- TASK_SUBMIT
        elseif msg_type == 0x50 then
            if payload_len >= 8 then
                payload_tree:add_le(pf_task_id, payload_buf(0, 4))
                payload_tree:add(pf_task_delivery, payload_buf(4, 1))
                payload_tree:add_le(pf_task_args_len, payload_buf(6, 2))
            end

        -- TASK_ACCEPT / TASK_REJECT
        elseif msg_type == 0x51 or msg_type == 0x52 then
            if payload_len >= 16 then
                payload_tree:add_le(pf_task_id, payload_buf(0, 4))
                payload_tree:add(pf_task_status, payload_buf(4, 1))
                payload_tree:add_le(pf_task_remote_pid, payload_buf(8, 8))
            end

        -- TASK_COMPLETE
        elseif msg_type == 0x53 then
            if payload_len >= 12 then
                payload_tree:add_le(pf_task_id, payload_buf(0, 4))
                payload_tree:add_le(pf_task_exit_status, payload_buf(4, 4))
                local output_len = payload_buf(8, 2):le_uint()
                payload_tree:add_le(pf_task_output_len, payload_buf(8, 2))
                if output_len > 0 and payload_len >= 12 + output_len then
                    payload_tree:add(pf_payload_data, payload_buf(12, output_len))
                end
            end

        -- TASK_CANCEL
        elseif msg_type == 0x54 then
            if payload_len >= 4 then
                payload_tree:add_le(pf_task_id, payload_buf(0, 4))
            end

        -- LOAD_REPORT
        elseif msg_type == 0x55 then
            if payload_len >= 8 then
                local num_cpus = payload_buf(0, 2):le_uint()
                payload_tree:add_le(pf_load_num_cpus, payload_buf(0, 2))
                payload_tree:add_le(pf_load_runnable, payload_buf(2, 2))
                payload_tree:add_le(pf_load_avg, payload_buf(4, 2))
                payload_tree:add_le(pf_load_free_mem, payload_buf(6, 2))

                -- Per-CPU loads
                local offset = 8
                for i = 1, num_cpus do
                    if offset + 2 <= payload_len then
                        local cpu_load = payload_buf(offset, 2):le_uint()
                        payload_tree:add(wki_proto, payload_buf(offset, 2),
                            string.format("CPU %d Load: %d (%.1f%%)", i - 1, cpu_load, cpu_load / 10.0))
                        offset = offset + 2
                    end
                end
            end

        else
            -- Unknown/unhandled message type - show raw payload
            payload_tree:add(pf_payload_data, payload_buf(0, payload_len))
        end
    end

    return length
end

-- Register with Ethernet dissector table
local eth_table = DissectorTable.get("ethertype")
eth_table:add(WKI_ETHERTYPE, wki_proto)

-- Also register for UDP port 88B7 (for testing over IP)
local udp_table = DissectorTable.get("udp.port")
udp_table:add(0x88B7, wki_proto)

print("WKI Protocol dissector loaded (EtherType 0x88B7)")
