#include "rte_common.h"
#include "rte_mbuf.h"
#include "rte_meter.h"
#include "rte_red.h"
#include "qos.h"

struct rte_red app_queue[APP_FLOWS_MAX];
uint64_t app_queue_size[APP_FLOWS_MAX] = {0};
struct rte_meter_srtcm app_flow[APP_FLOWS_MAX];

/*----------------- Parameter deduction STARTs here -----------------*/

struct rte_meter_srtcm_params app_srtcm_params[] = {
	{.cir = 1000000000000 * 0.16,  .cbs = 60000, .ebs = 50000},
};

struct rte_red_config app_red_params[APP_FLOWS_MAX][3] = {
	/* Flow 0 */
	[0][0] = {.min_th = 480 * 2000, .max_th = 640 * 2000, .maxp_inv = 10, .wq_log2 = 1},
	[0][1] = {.min_th = 400 * 2000, .max_th = 640 * 2000, .maxp_inv = 10, .wq_log2 = 1},
	[0][2] = {.min_th = 320 * 2000, .max_th = 640 * 2000, .maxp_inv = 10, .wq_log2 = 1},

	/* Flow 1 */
	[1][0] = {.min_th = 480 * 1200, .max_th = 640 * 1200, .maxp_inv = 10, .wq_log2 = 2},
	[1][1] = {.min_th = 400 * 1200, .max_th = 640 * 1200, .maxp_inv = 10, .wq_log2 = 2},
	[1][2] = {.min_th = 320 * 1200, .max_th = 640 * 1200, .maxp_inv = 10, .wq_log2 = 2},

	/* Flow 2 */
	[2][0] = {.min_th = 480 * 900, .max_th = 640 * 900, .maxp_inv = 10, .wq_log2 = 3},
	[2][1] = {.min_th = 400 * 900, .max_th = 640 * 900, .maxp_inv = 10, .wq_log2 = 3},
	[2][2] = {.min_th = 320 * 900, .max_th = 640 * 900, .maxp_inv = 10, .wq_log2 = 3},

	/* Flow 3 */
	[3][0] = {.min_th = 480 * 600, .max_th = 640 * 600, .maxp_inv = 10, .wq_log2 = 4},
	[3][1] = {.min_th = 400 * 600, .max_th = 640 * 600, .maxp_inv = 10, .wq_log2 = 4},
	[3][2] = {.min_th = 320 * 600, .max_th = 640 * 600, .maxp_inv = 10, .wq_log2 = 4},
};

/*----------------- Parameter deduction ENDs here -----------------*/

/*----------------- Implementation of srTCM STARTs here -----------------*/

int qos_meter_init(void) {
	int ret;
    for (int i = 0, j = 0; i < APP_FLOWS_MAX; ++i, j = (j + 1) % RTE_DIM(app_srtcm_params)) {
		ret = rte_meter_srtcm_config(&app_flow[i], &app_srtcm_params[j]);
		/* Stop if error occurs */
		if (ret) {
			return ret;
		}
	}
    return 0;
}

enum qos_color qos_meter_run(uint32_t flow_id, uint32_t pkt_len, uint64_t time) {
    return rte_meter_srtcm_color_blind_check(&app_flow[flow_id], time, pkt_len);
}

/*----------------- Implementation of srTCM ENDs here -----------------*/

/*----------------- Implementation of WRED STARTs here -----------------*/

int qos_dropper_init(void) {
	int ret;
    for(int i = 0; i < APP_FLOWS_MAX; ++i) {
		ret = rte_red_rt_data_init(&app_queue[i]);
		/* Stop if error occurs */
		if (ret != 0) {
			return ret;
		}
	}
	return 0;
}

int qos_dropper_run(uint32_t flow_id, enum qos_color color, uint64_t time) {
	/* Empty the queue at the end of the time period */
	if (time != app_queue[flow_id].q_time) {
		rte_red_mark_queue_empty(&app_queue[flow_id], time);
		app_queue_size[flow_id] = 0;
	}

	/* Make enqueue operation */
    int ret = rte_red_enqueue(&app_red_params[flow_id][color], &app_queue[flow_id], app_queue_size[flow_id], time);
	if (ret) {
		return 1;
	}
	else {
		++app_queue_size[flow_id];
		return 0;
	}
}

/*----------------- Implementation of WRED ENDs here -----------------*/
