sudo modprobe uio # 挂载 Linux 内核的 UIO 模块
sudo insmod ./build/kmod/igb_uio.ko # 挂载编译生成的 igb_uio 模块
sudo ./tools/dpdk-devbind.py -s # 列出所有网卡
sudo ./tools/dpdk-devbind.py \ # 为 1.3 中添加的网卡绑定 igb_uio 驱动
--bind=igb_uio enoX # 其中 enoX 由具体环境决定(enoX 一般为 02:00.1 之类的)
mkdir -p /mnt/huge # 创建 hugetlbfs 挂载点
mount -t hugetlbfs nodev /mnt/huge # 挂载 hugetlbfs
echo 64 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages # 分配大页 