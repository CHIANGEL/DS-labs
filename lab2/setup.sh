sudo modprobe uio
sudo insmod ./x86_64-native-linuxapp-gcc/kmod/igb_uio.ko 
sudo ./tools/dpdk-devbind.py -s
sudo ./tools/dpdk-devbind.py --bind=igb_uio 02:01.0
mkdir -p /mnt/huge
mount -t hugetlbfs nodev /mnt/huge
echo 64 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
sudo ./tools/dpdk-devbind.py -s
