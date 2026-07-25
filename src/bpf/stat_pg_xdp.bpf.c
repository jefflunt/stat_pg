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

    unsigned char *curr = payload + 4;

    #pragma clang loop unroll(disable)
    for (int i = 0; i < 32; i++) {
        if ((void *)(curr + 4) > data_end)
            break;

        __u32 id = curr[0] | ((__u32)curr[1] << 8);
        __u16 val = curr[2] | ((__u16)curr[3] << 8);
        curr += 4;

        if (id > 0 && id < 65536) {
            __u64 *v = bpf_map_lookup_elem(&metrics_map, &id);
            if (v) {
                *v += val;
            }
        }
    }

    return XDP_DROP;
}

char _license[] SEC("license") = "GPL";
