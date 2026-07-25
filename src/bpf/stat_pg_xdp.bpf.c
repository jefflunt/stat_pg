#include <linux/bpf.h>
#include <linux/if_ether.h>
#include <linux/ip.h>
#include <linux/in.h>
#include <linux/udp.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 65536);
    __type(key, __u32);
    __type(value, __u64);
} metrics_map SEC(".maps");

SEC("xdp")
int stat_pg_xdp_prog(struct xdp_md *ctx) {
    void *data = (void *)(long)ctx->data;
    void *data_end = (void *)(long)ctx->data_end;

    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end)
        return XDP_PASS;

    if (eth->h_proto != bpf_htons(ETH_P_IP))
        return XDP_PASS;

    struct iphdr *iph = (void *)(eth + 1);
    if ((void *)(iph + 1) > data_end)
        return XDP_PASS;

    if (iph->protocol != IPPROTO_UDP)
        return XDP_PASS;

    struct udphdr *udph = (void *)((char *)iph + (iph->ihl * 4));
    if ((void *)(udph + 1) > data_end)
        return XDP_PASS;

    if (udph->dest != bpf_htons(8125))
        return XDP_PASS;

    unsigned char *payload = (unsigned char *)(udph + 1);
    if ((void *)(payload + 4) > data_end)
        return XDP_PASS;

    unsigned char fmt = payload[1];
    __u16 num_updates = payload[2] | ((__u16)payload[3] << 8);
    unsigned char *curr = payload + 4;

    if (fmt == 0) { // 16-bit ID INC (+1, 2 bytes)
        #pragma clang loop unroll(disable)
        for (int i = 0; i < 64; i++) {
            if (i >= num_updates) break;
            if ((void *)(curr + 2) > data_end) break;
            __u32 id = curr[0] | ((__u32)curr[1] << 8);
            curr += 2;
            if (id > 0 && id < 65536) {
                __u64 *v = bpf_map_lookup_elem(&metrics_map, &id);
                if (v) { *v += 1; }
            }
        }
    } else if (fmt == 1) { // 16-bit ID DEC (-1, 2 bytes)
        #pragma clang loop unroll(disable)
        for (int i = 0; i < 64; i++) {
            if (i >= num_updates) break;
            if ((void *)(curr + 2) > data_end) break;
            __u32 id = curr[0] | ((__u32)curr[1] << 8);
            curr += 2;
            if (id > 0 && id < 65536) {
                __u64 *v = bpf_map_lookup_elem(&metrics_map, &id);
                if (v && *v > 0) { *v -= 1; }
            }
        }
    } else if (fmt == 2 || fmt == 3) { // 16-bit ID, 8-bit val (3 bytes)
        #pragma clang loop unroll(disable)
        for (int i = 0; i < 64; i++) {
            if (i >= num_updates) break;
            if ((void *)(curr + 3) > data_end) break;
            __u32 id = curr[0] | ((__u32)curr[1] << 8);
            __u8 val = curr[2];
            curr += 3;
            if (id > 0 && id < 65536) {
                __u64 *v = bpf_map_lookup_elem(&metrics_map, &id);
                if (v) { *v += val; }
            }
        }
    } else if (fmt >= 4 && fmt <= 7) { // 16-bit ID, 16-bit val (4 bytes)
        #pragma clang loop unroll(disable)
        for (int i = 0; i < 64; i++) {
            if (i >= num_updates) break;
            if ((void *)(curr + 4) > data_end) break;
            __u32 id = curr[0] | ((__u32)curr[1] << 8);
            __u16 val = curr[2] | ((__u16)curr[3] << 8);
            curr += 4;
            if (id > 0 && id < 65536) {
                __u64 *v = bpf_map_lookup_elem(&metrics_map, &id);
                if (v) { *v += val; }
            }
        }
    } else if (fmt >= 8 && fmt <= 11) { // 16-bit ID, 32-bit val (6 bytes)
        #pragma clang loop unroll(disable)
        for (int i = 0; i < 64; i++) {
            if (i >= num_updates) break;
            if ((void *)(curr + 6) > data_end) break;
            __u32 id = curr[0] | ((__u32)curr[1] << 8);
            __u32 val = curr[2] | ((__u32)curr[3] << 8) | ((__u32)curr[4] << 16) | ((__u32)curr[5] << 24);
            curr += 6;
            if (id > 0 && id < 65536) {
                __u64 *v = bpf_map_lookup_elem(&metrics_map, &id);
                if (v) { *v += val; }
            }
        }
    } else if (fmt >= 12 && fmt <= 15) { // 16-bit ID, 64-bit val (10 bytes)
        #pragma clang loop unroll(disable)
        for (int i = 0; i < 64; i++) {
            if (i >= num_updates) break;
            if ((void *)(curr + 10) > data_end) break;
            __u32 id = curr[0] | ((__u32)curr[1] << 8);
            __u64 val = curr[2] | ((__u64)curr[3] << 8) | ((__u64)curr[4] << 16) | ((__u64)curr[5] << 24) |
                        ((__u64)curr[6] << 32) | ((__u64)curr[7] << 40) | ((__u64)curr[8] << 48) | ((__u64)curr[9] << 56);
            curr += 10;
            if (id > 0 && id < 65536) {
                __u64 *v = bpf_map_lookup_elem(&metrics_map, &id);
                if (v) { *v += val; }
            }
        }
    }

    return XDP_DROP;
}

char _license[] SEC("license") = "GPL";
