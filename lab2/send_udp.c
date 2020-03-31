#include <rte_mbuf.h>
#include <rte_ip.h>
#include <rte_ethdev.h>
#include <rte_ether.h>
#include <rte_eal.h>
#include <rte_lcore.h>
#include <rte_cycles.h>
#include <rte_udp.h>
#include <stdint.h>
#include <inttypes.h>

#define RX_RING_SIZE 128
#define TX_RING_SIZE 512

#define NUM_MBUFS 8191
#define MBUF_CACHE_SIZE 250
#define BURST_SIZE 32

static const struct rte_eth_conf port_conf_default = {
	.rxmode = { .max_rx_pkt_len = ETHER_MAX_LEN }
};

/*
 * port_init():
 *   - Initializes a port with global settings the RX buffers.
 *   - Simply copied from the example/skeleton in DPDK
 */
static inline int port_init(uint8_t port, struct rte_mempool *mbuf_pool)
{
	struct rte_eth_conf port_conf = port_conf_default;
	const uint16_t rx_rings = 1, tx_rings = 1;
	int retval;
	uint16_t q;

	if (port >= rte_eth_dev_count())
		return -1;

	/* Configure the Ethernet device. */
	retval = rte_eth_dev_configure(port, rx_rings, tx_rings, &port_conf);
	if (retval != 0)
		return retval;

	/* Allocate and set up 1 RX queue per Ethernet port. */
	for (q = 0; q < rx_rings; q++) {
		retval = rte_eth_rx_queue_setup(port, q, RX_RING_SIZE,
				rte_eth_dev_socket_id(port), NULL, mbuf_pool);
		if (retval < 0)
			return retval;
	}

	/* Allocate and set up 1 TX queue per Ethernet port. */
	for (q = 0; q < tx_rings; q++) {
		retval = rte_eth_tx_queue_setup(port, q, TX_RING_SIZE,
				rte_eth_dev_socket_id(port), NULL);
		if (retval < 0)
			return retval;
	}

	/* Start the Ethernet port. */
	retval = rte_eth_dev_start(port);
	if (retval < 0)
		return retval;

	/* Display the port MAC address. */
	struct ether_addr addr;
	rte_eth_macaddr_get(port, &addr);
	printf("Port %u MAC: %02" PRIx8 " %02" PRIx8 " %02" PRIx8
			   " %02" PRIx8 " %02" PRIx8 " %02" PRIx8 "\n",
			(unsigned)port,
			addr.addr_bytes[0], addr.addr_bytes[1],
			addr.addr_bytes[2], addr.addr_bytes[3],
			addr.addr_bytes[4], addr.addr_bytes[5]);

	/* Enable RX in promiscuous mode for the Ethernet device. */
	rte_eth_promiscuous_enable(port);

	return 0;
}

/* 
 * build_udp_packet:
 *   - The fucntion to build an UDP packet.
 *   - Params:
 *       - m: a pointer, which points to an rte_mbuf object
 *       - data: the payload of the UDP packet
 *       - data_len: the length of the data
 */
static void build_udp_packet(struct rte_mbuf *m, const char* data, uint32_t data_len) {
	struct ether_hdr *ether_header;
    struct ipv4_hdr *ipv4_header;
	struct udp_hdr *udp_header;
	
	/* Initialize the pointers */
	ether_header = rte_pktmbuf_mtod(m, struct ether_hdr *);
    ipv4_header = rte_pktmbuf_mtod_offset(m, struct ipv4_hdr *, sizeof(struct ether_hdr));
	udp_header = rte_pktmbuf_mtod_offset(m, struct udp_hdr *, sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr));

	/* Fill the UDP header */
	udp_header->src_port = 0xff << 8;
	udp_header->dst_port = 0xfe << 8;
	udp_header->dgram_len = (data_len + sizeof(struct udp_hdr)) << 8;
	udp_header->dgram_cksum = 0xffff;

	/* Fill the ethernet header */
	struct ether_addr s_addr_tmp, d_addr_tmp;
	rte_eth_macaddr_get(0, &s_addr_tmp);
	rte_eth_macaddr_get(0, &d_addr_tmp);
	ether_header->s_addr = s_addr_tmp;
	ether_header->d_addr = d_addr_tmp;
	ether_header->ether_type = rte_be_to_cpu_16(ETHER_TYPE_IPv4);

	/* Fill the ipv4 header */
	ipv4_header->version_ihl = (4 << 4) | 5;
	ipv4_header->type_of_service = 0;
	ipv4_header->src_addr = IPv4(10, 80, 168, 192);
	ipv4_header->dst_addr = IPv4(6, 80, 168, 192);
	ipv4_header->total_length = 38 << 8 ;
	ipv4_header->packet_id = 15 << 8;
	ipv4_header->fragment_offset = 128;
	ipv4_header->time_to_live = 0xff;
	ipv4_header->next_proto_id = 0x11;
	ipv4_header->hdr_checksum = rte_ipv4_cksum(ipv4_header);

	/* Fill the UDP payload */
	void *payload = rte_pktmbuf_mtod_offset(m, void *, sizeof(struct udp_hdr) + sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr));
	memcpy(payload, data, data_len);
}

/*
 * lcore_main:
 *   - The main function for each lcore, which reads from an input port and writes to an output port.
 */
static __attribute__((noreturn)) void lcore_main(void)
{
	uint8_t port;
	struct rte_mempool *mbuf_pool;
	const uint8_t nb_ports = rte_eth_dev_count();

	/* Create a mempool to hold the mbufs. */
	mbuf_pool = rte_pktmbuf_pool_create("MY_BUF_POOL", NUM_MBUFS * nb_ports,
		MBUF_CACHE_SIZE, 0, RTE_MBUF_DEFAULT_BUF_SIZE, rte_socket_id());
    
	/* Check whether the port is on the same NUMA node as the polling thread. */
	for (port = 0; port < nb_ports; port++) {
		if (rte_eth_dev_socket_id(port) > 0 && rte_eth_dev_socket_id(port) != (int)rte_socket_id()) {
			printf("WARNING, port %u is on remote NUMA node to polling thread.\n\tPerformance will not be optimal.\n", port);
        }
    }
	printf("\nCore %u forwarding packets. [Ctrl+C to quit]\n", rte_lcore_id());

	/* Run until the application is quit or killed. */
	struct rte_mbuf *packets[1];
	packets[0] = rte_pktmbuf_alloc(mbuf_pool);
	rte_pktmbuf_prepend(packets[0], sizeof(struct udp_hdr) + sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr) + 10);
	build_udp_packet(packets[0], "LinJianghao", 11);
	for (;;) {
		const uint16_t nb_tx = rte_eth_tx_burst(0, 0, packets, 1);
		printf("Successfully send an UDP packet: %d\n", nb_tx);
		sleep(1);
	}
}

/*
 * main:
 *   - Initializes the environment and calls the functions per-lcore.
 */
int main(int argc, char *argv[])
{
	uint8_t portid;
	unsigned nb_ports;
	struct rte_mempool *mbuf_pool;

	/* Initialize the Environment Abstraction Layer (EAL). */
	int ret = rte_eal_init(argc, argv);
	if (ret < 0) {
        rte_exit(EXIT_FAILURE, "Error with EAL initialization\n");
    }
    else {
        argc -= ret;
        argv += ret;
    }

	/* Check that there is an even number of ports to send/receive on. */
	nb_ports = rte_eth_dev_count();
	printf("port num:%d\n", nb_ports);

	/* Creates a new mempool in memory to hold the mbufs. */
	mbuf_pool = rte_pktmbuf_pool_create("MBUF_POOL", NUM_MBUFS * nb_ports,
		MBUF_CACHE_SIZE, 0, RTE_MBUF_DEFAULT_BUF_SIZE, rte_socket_id());
	if (mbuf_pool == NULL) {
		rte_exit(EXIT_FAILURE, "Cannot create mbuf pool\n");
    }

	/* Initialize all ports. */
	for (portid = 0; portid < nb_ports; portid++) {
		if (port_init(portid, mbuf_pool) != 0) {
			rte_exit(EXIT_FAILURE, "Cannot init port %"PRIu8 "\n", portid);
        }
    }
	if (rte_lcore_count() > 1) {
		printf("\nWARNING: Too many lcores enabled. Only 1 used.\n");
    }

	/* Call lcore_main on the master core only. */
	lcore_main();

	return 0;
}
