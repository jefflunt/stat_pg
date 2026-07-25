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

    unsigned char *curr = payload + 1;
    unsigned char current_fmt = payload[1];

    #pragma clang loop unroll(disable)
    for (int step = 0; step < 8; step++) {
        if ((void *)(curr + 4) > data_end)
            break;

        unsigned char header_byte = curr[0];
        if (header_byte < 16) {
            current_fmt = header_byte;
            curr += 3;
            if ((void *)(curr + 4) > data_end)
                break;
        }

        if (current_fmt == 0) { // INC format: 2 bytes per update (u16 id, implicit +1)
            __u32 id = curr[0] | ((__u32)curr[1] << 8);
            curr += 2;

            if (id > 0 && id < 65536) {
                __u64 *v = bpf_map_lookup_elem(&metrics_map, &id);
                if (v) { *v += 1; }
            }
        } else if (current_fmt == 1) { // DEC format: 2 bytes per update (u16 id, implicit -1)
            __u32 id = curr[0] | ((__u32)curr[1] << 8);
            curr += 2;

            if (id > 0 && id < 65536) {
                __u64 *v = bpf_map_lookup_elem(&metrics_map, &id);
                if (v && *v > 0) { *v -= 1; }
            }
        } else { // VALUE formats (4 bytes: u16 id, u16 val)
            __u32 id = curr[0] | ((__u32)curr[1] << 8);
            __u16 val = curr[2] | ((__u16)curr[3] << 8);
            curr += 4;

            if (id > 0 && id < 65536) {
                __u64 *v = bpf_map_lookup_elem(&metrics_map, &id);
                if (v) { *v += val; }
            }
        }
    }

    return XDP_DROP;
}

char _license[] SEC("license") = "GPL";
