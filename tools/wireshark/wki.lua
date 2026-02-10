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

-- Analysis fields (computed by dissector, not on wire)
local pf_analysis_retransmit = ProtoField.bool("wki.analysis.retransmit", "Retransmission")
local pf_analysis_dup_ack = ProtoField.bool("wki.analysis.dup_ack", "Duplicate ACK")
local pf_analysis_out_of_order = ProtoField.bool("wki.analysis.out_of_order", "Out of Order")
local pf_analysis_rtt = ProtoField.double("wki.analysis.rtt", "Round Trip Time (ms)")
local pf_analysis_req_frame = ProtoField.framenum("wki.analysis.req_frame", "Request Frame", base.NONE, frametype.REQUEST)
local pf_analysis_resp_frame = ProtoField.framenum("wki.analysis.resp_frame", "Response Frame", base.NONE, frametype.RESPONSE)
local pf_analysis_resp_time = ProtoField.double("wki.analysis.response_time", "Response Time (ms)")
local pf_analysis_credit_zero = ProtoField.bool("wki.analysis.credit_zero", "Zero Credits (Stalled)")

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
    -- Analysis
    pf_analysis_retransmit, pf_analysis_dup_ack, pf_analysis_out_of_order,
    pf_analysis_rtt, pf_analysis_req_frame, pf_analysis_resp_frame,
    pf_analysis_resp_time, pf_analysis_credit_zero,
}

-- =========================================================================
-- Analysis State (tracks seq numbers, request-response pairs, per-channel)
-- =========================================================================

-- Per-direction stream state: keyed by "src_node:dst_node:channel_id"
-- Tracks expected next seq to detect retransmits and out-of-order
local stream_state = {}  -- [stream_key] = { next_seq, seen_seqs={} }

-- Request-response correlation: keyed by "src_node:dst_node:channel_id:seq_num"
-- For DEV_OP_REQ (0x43) -> DEV_OP_RESP (0x44) matching
local pending_requests = {}  -- [key] = { frame_num, timestamp, op_id }
-- Reverse map: response frame -> request info
local response_info = {}  -- [frame_num] = { req_frame, latency_ms, op_id }
-- Forward map: request frame -> response info
local request_info = {}  -- [frame_num] = { resp_frame }

-- ACK tracking: keyed by "src_node:dst_node:channel_id:seq_num"
-- When a data frame is sent, record timestamp. When ACK arrives, compute RTT.
local sent_frames = {}  -- [key] = { frame_num, timestamp }
local ack_rtt_info = {} -- [frame_num] = { rtt_ms }

-- Per-peer statistics
local peer_stats = {}  -- [node_id] = { tx_frames, rx_frames, tx_bytes, rx_bytes, retransmits }
-- Per-channel statistics
local channel_stats = {} -- [stream_key] = { msg_counts={}, total_frames, credits_zero_count }
-- Per-operation latency tracking
local op_latencies = {} -- [op_name] = { values={}, count, sum, min, max }

-- REQ/RESP message type pairs for correlation
local req_resp_pairs = {
    [0x43] = 0x44,  -- DEV_OP_REQ -> DEV_OP_RESP
    [0x40] = 0x41,  -- DEV_ATTACH_REQ -> DEV_ATTACH_ACK
    [0x20] = 0x21,  -- ZONE_CREATE_REQ -> ZONE_CREATE_ACK
    [0x25] = 0x26,  -- ZONE_READ_REQ -> ZONE_READ_RESP
    [0x27] = 0x28,  -- ZONE_WRITE_REQ -> ZONE_WRITE_ACK
    [0x50] = 0x51,  -- TASK_SUBMIT -> TASK_ACCEPT (or TASK_REJECT)
    [0x46] = 0x47,  -- CHANNEL_OPEN -> CHANNEL_OPEN_ACK
}
-- Build reverse map
local resp_req_pairs = {}
for req, resp in pairs(req_resp_pairs) do
    resp_req_pairs[resp] = req
end
-- TASK_SUBMIT can also get TASK_REJECT
resp_req_pairs[0x52] = 0x50

local function reset_analysis_state()
    stream_state = {}
    pending_requests = {}
    response_info = {}
    request_info = {}
    sent_frames = {}
    ack_rtt_info = {}
    peer_stats = {}
    channel_stats = {}
    op_latencies = {}
end

local function ensure_peer_stats(node_id)
    if not peer_stats[node_id] then
        peer_stats[node_id] = {
            tx_frames = 0, rx_frames = 0,
            tx_bytes = 0, rx_bytes = 0,
            retransmits = 0, rtt_sum = 0, rtt_count = 0
        }
    end
    return peer_stats[node_id]
end

local function ensure_channel_stats(key)
    if not channel_stats[key] then
        channel_stats[key] = {
            msg_counts = {}, total_frames = 0,
            credits_zero_count = 0, total_bytes = 0
        }
    end
    return channel_stats[key]
end

local function record_op_latency(op_name, latency_ms)
    if not op_latencies[op_name] then
        op_latencies[op_name] = { values = {}, count = 0, sum = 0, min = math.huge, max = 0 }
    end
    local entry = op_latencies[op_name]
    table.insert(entry.values, latency_ms)
    entry.count = entry.count + 1
    entry.sum = entry.sum + latency_ms
    if latency_ms < entry.min then entry.min = latency_ms end
    if latency_ms > entry.max then entry.max = latency_ms end
end

local function compute_percentile(sorted_values, pct)
    if #sorted_values == 0 then return 0 end
    local idx = math.ceil(#sorted_values * pct / 100)
    if idx < 1 then idx = 1 end
    if idx > #sorted_values then idx = #sorted_values end
    return sorted_values[idx]
end

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

    -- =====================================================================
    -- Analysis: retransmit/out-of-order detection, request-response matching
    -- =====================================================================
    local is_retransmit = false
    local is_dup_ack = false
    local is_out_of_order = false
    local is_credit_zero = (credits == 0)
    local matched_rtt_ms = nil
    local matched_resp_time_ms = nil
    local matched_req_frame = nil
    local matched_resp_frame = nil

    -- Only run analysis on first pass (not when user clicks on a packet)
    if not pinfo.visited then
        local fwd_key = string.format("%d:%d:%d", src_node, dst_node, channel_id)
        local frame_num = pinfo.number
        local timestamp = pinfo.abs_ts

        -- Update peer statistics
        local src_stats = ensure_peer_stats(src_node)
        src_stats.tx_frames = src_stats.tx_frames + 1
        src_stats.tx_bytes = src_stats.tx_bytes + payload_len
        local dst_stats = ensure_peer_stats(dst_node)
        dst_stats.rx_frames = dst_stats.rx_frames + 1
        dst_stats.rx_bytes = dst_stats.rx_bytes + payload_len

        -- Update channel statistics
        local ch_stats = ensure_channel_stats(fwd_key)
        ch_stats.total_frames = ch_stats.total_frames + 1
        ch_stats.total_bytes = ch_stats.total_bytes + payload_len
        ch_stats.msg_counts[msg_type] = (ch_stats.msg_counts[msg_type] or 0) + 1
        if is_credit_zero then
            ch_stats.credits_zero_count = ch_stats.credits_zero_count + 1
        end

        -- Sequence analysis: detect retransmits and out-of-order for reliable messages
        -- (skip unreliable control messages: HELLO, HELLO_ACK, HEARTBEAT, HEARTBEAT_ACK)
        if msg_type ~= 0x01 and msg_type ~= 0x02 and msg_type ~= 0x03 and msg_type ~= 0x04 then
            if not stream_state[fwd_key] then
                stream_state[fwd_key] = { next_seq = seq_num, seen_seqs = {} }
            end
            local state = stream_state[fwd_key]

            if state.seen_seqs[seq_num] then
                -- Already saw this seq on this stream: retransmit
                is_retransmit = true
                src_stats.retransmits = src_stats.retransmits + 1
            else
                state.seen_seqs[seq_num] = frame_num
                if seq_num ~= state.next_seq and seq_num > 0 then
                    -- Arrived out of order (seq > expected)
                    if seq_num > state.next_seq then
                        is_out_of_order = true
                    end
                end
                if seq_num >= state.next_seq then
                    state.next_seq = seq_num + 1
                end
            end

            -- Track sent frame timestamps for RTT calculation
            sent_frames[fwd_key .. ":" .. seq_num] = { frame_num = frame_num, timestamp = timestamp }
        end

        -- ACK processing: if ACK_PRESENT, compute RTT for the acked frame
        if bit.band(flags, 0x08) ~= 0 then
            -- The ACK is from src_node to dst_node, acknowledging dst_node's seq
            -- Reverse key: original sender was dst_node -> src_node on this channel
            local rev_key = string.format("%d:%d:%d", dst_node, src_node, channel_id)
            local ack_sent_key = rev_key .. ":" .. ack_num
            local original = sent_frames[ack_sent_key]
            if original then
                local rtt_ms = (timestamp - original.timestamp) * 1000
                ack_rtt_info[frame_num] = { rtt_ms = rtt_ms }
                matched_rtt_ms = rtt_ms
                -- Update peer RTT stats
                dst_stats.rtt_sum = dst_stats.rtt_sum + rtt_ms
                dst_stats.rtt_count = dst_stats.rtt_count + 1
            end
        end

        -- Request-Response correlation
        if req_resp_pairs[msg_type] then
            -- This is a request message, record it
            local op_id = nil
            if (msg_type == 0x43 or msg_type == 0x44) and payload_len >= 2 then
                op_id = buffer(WKI_HEADER_SIZE, 2):le_uint()
            end
            -- Key: responder will send from dst_node to src_node on same channel
            local resp_key = string.format("%d:%d:%d:%d", dst_node, src_node, channel_id, msg_type)
            pending_requests[resp_key] = {
                frame_num = frame_num, timestamp = timestamp, op_id = op_id
            }
        elseif resp_req_pairs[msg_type] then
            -- This is a response, find the matching request
            local req_type = resp_req_pairs[msg_type]
            local req_key = string.format("%d:%d:%d:%d", src_node, dst_node, channel_id, req_type)
            local req = pending_requests[req_key]
            if req then
                local latency_ms = (timestamp - req.timestamp) * 1000
                response_info[frame_num] = {
                    req_frame = req.frame_num, latency_ms = latency_ms, op_id = req.op_id
                }
                request_info[req.frame_num] = { resp_frame = frame_num }
                matched_resp_time_ms = latency_ms
                matched_req_frame = req.frame_num

                -- Record per-operation latency
                local op_name = msg_types[req_type] or "UNKNOWN"
                if req.op_id then
                    op_name = op_ids[req.op_id] or string.format("OP_0x%04X", req.op_id)
                end
                record_op_latency(op_name, latency_ms)

                pending_requests[req_key] = nil  -- consumed
            end
        end
    else
        -- Visited: retrieve cached analysis results
        local frame_num = pinfo.number
        if ack_rtt_info[frame_num] then
            matched_rtt_ms = ack_rtt_info[frame_num].rtt_ms
        end
        if response_info[frame_num] then
            matched_resp_time_ms = response_info[frame_num].latency_ms
            matched_req_frame = response_info[frame_num].req_frame
        end
        if request_info[frame_num] then
            matched_resp_frame = request_info[frame_num].resp_frame
        end
    end

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

    -- Analysis subtree
    local has_analysis = is_retransmit or is_dup_ack or is_out_of_order or
                         matched_rtt_ms or matched_resp_time_ms or
                         matched_req_frame or matched_resp_frame or is_credit_zero
    if has_analysis then
        local analysis_tree = subtree:add(wki_proto, buffer(0, 0), "Analysis")
        if is_retransmit then
            local item = analysis_tree:add(pf_analysis_retransmit, true)
            item:set_generated(true)
            item:add_expert_info(PI_SEQUENCE, PI_NOTE, "WKI retransmission detected")
            pinfo.cols.info:append(" [Retransmission]")
        end
        if is_dup_ack then
            local item = analysis_tree:add(pf_analysis_dup_ack, true)
            item:set_generated(true)
        end
        if is_out_of_order then
            local item = analysis_tree:add(pf_analysis_out_of_order, true)
            item:set_generated(true)
            item:add_expert_info(PI_SEQUENCE, PI_WARN, "WKI out-of-order packet")
            pinfo.cols.info:append(" [Out-of-Order]")
        end
        if matched_rtt_ms then
            local item = analysis_tree:add(pf_analysis_rtt, matched_rtt_ms)
            item:set_generated(true)
            item:append_text(string.format(" (%.3f ms)", matched_rtt_ms))
        end
        if matched_req_frame then
            local item = analysis_tree:add(pf_analysis_req_frame, matched_req_frame)
            item:set_generated(true)
        end
        if matched_resp_frame then
            local item = analysis_tree:add(pf_analysis_resp_frame, matched_resp_frame)
            item:set_generated(true)
        end
        if matched_resp_time_ms then
            local item = analysis_tree:add(pf_analysis_resp_time, matched_resp_time_ms)
            item:set_generated(true)
            item:append_text(string.format(" (%.3f ms)", matched_resp_time_ms))
            pinfo.cols.info:append(string.format(" [RTT=%.3fms]", matched_resp_time_ms))
        end
        if is_credit_zero then
            local item = analysis_tree:add(pf_analysis_credit_zero, true)
            item:set_generated(true)
            item:add_expert_info(PI_SEQUENCE, PI_WARN, "Zero credits - sender may be stalled")
        end
    end

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

-- =========================================================================
-- Statistics Window (Statistics -> WKI Protocol Statistics)
-- =========================================================================

local function show_wki_statistics()
    local win = TextWindow.new("WKI Protocol Statistics")
    local lines = {}

    -- Header
    table.insert(lines, "=" .. string.rep("=", 78))
    table.insert(lines, "  WKI Protocol Statistics")
    table.insert(lines, "=" .. string.rep("=", 78))

    -- Per-Peer Statistics
    table.insert(lines, "")
    table.insert(lines, "--- Per-Peer Summary ---")
    table.insert(lines, string.format("  %-12s %8s %8s %10s %10s %8s %10s",
        "Node", "TX Frm", "RX Frm", "TX Bytes", "RX Bytes", "Retrans", "Avg RTT"))
    table.insert(lines, "  " .. string.rep("-", 72))

    local sorted_peers = {}
    for node_id, _ in pairs(peer_stats) do
        table.insert(sorted_peers, node_id)
    end
    table.sort(sorted_peers)

    for _, node_id in ipairs(sorted_peers) do
        local s = peer_stats[node_id]
        local avg_rtt = s.rtt_count > 0 and string.format("%.3f ms", s.rtt_sum / s.rtt_count) or "N/A"
        table.insert(lines, string.format("  %-12s %8d %8d %10d %10d %8d %10s",
            format_node_id(node_id), s.tx_frames, s.rx_frames,
            s.tx_bytes, s.rx_bytes, s.retransmits, avg_rtt))
    end

    -- Per-Channel Statistics
    table.insert(lines, "")
    table.insert(lines, "--- Per-Channel Summary ---")
    table.insert(lines, string.format("  %-30s %8s %10s %8s",
        "Stream (src:dst:ch)", "Frames", "Bytes", "Cred=0"))
    table.insert(lines, "  " .. string.rep("-", 60))

    local sorted_channels = {}
    for key, _ in pairs(channel_stats) do
        table.insert(sorted_channels, key)
    end
    table.sort(sorted_channels)

    for _, key in ipairs(sorted_channels) do
        local s = channel_stats[key]
        table.insert(lines, string.format("  %-30s %8d %10d %8d",
            key, s.total_frames, s.total_bytes, s.credits_zero_count))
        -- Show message type breakdown
        local sorted_types = {}
        for mt, _ in pairs(s.msg_counts) do
            table.insert(sorted_types, mt)
        end
        table.sort(sorted_types)
        for _, mt in ipairs(sorted_types) do
            local mt_name = msg_types[mt] or string.format("0x%02X", mt)
            table.insert(lines, string.format("    %-26s %8d", mt_name, s.msg_counts[mt]))
        end
    end

    -- Per-Operation Latency Statistics
    table.insert(lines, "")
    table.insert(lines, "--- Request-Response Latency ---")
    table.insert(lines, string.format("  %-20s %8s %10s %10s %10s %10s %10s",
        "Operation", "Count", "Avg (ms)", "Min (ms)", "p50 (ms)", "p99 (ms)", "Max (ms)"))
    table.insert(lines, "  " .. string.rep("-", 82))

    local sorted_ops = {}
    for op_name, _ in pairs(op_latencies) do
        table.insert(sorted_ops, op_name)
    end
    table.sort(sorted_ops)

    for _, op_name in ipairs(sorted_ops) do
        local entry = op_latencies[op_name]
        if entry.count > 0 then
            -- Sort values for percentile calculation
            local sorted = {}
            for _, v in ipairs(entry.values) do table.insert(sorted, v) end
            table.sort(sorted)

            local avg = entry.sum / entry.count
            local p50 = compute_percentile(sorted, 50)
            local p99 = compute_percentile(sorted, 99)

            table.insert(lines, string.format("  %-20s %8d %10.3f %10.3f %10.3f %10.3f %10.3f",
                op_name, entry.count, avg, entry.min, p50, p99, entry.max))
        end
    end

    -- p99.9 latency summary (across all ops)
    table.insert(lines, "")
    table.insert(lines, "--- Overall Latency Percentiles ---")
    local all_latencies = {}
    for _, entry in pairs(op_latencies) do
        for _, v in ipairs(entry.values) do
            table.insert(all_latencies, v)
        end
    end
    if #all_latencies > 0 then
        table.sort(all_latencies)
        local total = #all_latencies
        table.insert(lines, string.format("  Total request-response pairs: %d", total))
        table.insert(lines, string.format("  p50:   %.3f ms", compute_percentile(all_latencies, 50)))
        table.insert(lines, string.format("  p90:   %.3f ms", compute_percentile(all_latencies, 90)))
        table.insert(lines, string.format("  p99:   %.3f ms", compute_percentile(all_latencies, 99)))
        table.insert(lines, string.format("  p99.9: %.3f ms", compute_percentile(all_latencies, 99.9)))
        table.insert(lines, string.format("  Min:   %.3f ms", all_latencies[1]))
        table.insert(lines, string.format("  Max:   %.3f ms", all_latencies[total]))
        table.insert(lines, string.format("  Avg:   %.3f ms",
            (function() local s=0; for _,v in ipairs(all_latencies) do s=s+v end; return s/total end)()))
    else
        table.insert(lines, "  No request-response pairs captured.")
    end

    table.insert(lines, "")
    table.insert(lines, "=" .. string.rep("=", 78))

    win:set(table.concat(lines, "\n"))
end

-- Register the statistics menu item
register_menu("WKI/Protocol Statistics", show_wki_statistics, MENU_TOOLS_UNSORTED)

-- Register a post-dissector listener to reset state on new capture
wki_proto.init = reset_analysis_state

-- Register with Ethernet dissector table
local eth_table = DissectorTable.get("ethertype")
eth_table:add(WKI_ETHERTYPE, wki_proto)

-- Also register for UDP port 88B7 (for testing over IP)
local udp_table = DissectorTable.get("udp.port")
udp_table:add(0x88B7, wki_proto)

print("WKI Protocol dissector loaded (EtherType 0x88B7) with statistics support")
