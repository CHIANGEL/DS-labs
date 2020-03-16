export RTE_SDK=/path/to/your/dpdk # 设置 DPDK SDK 路径
export RTE_TARGET=x86_64-native-linuxapp-gcc # 设置保存 DPDK 编译生成内容的目录(一般为 x86_64-native-linuxapp-gcc)
cd $RTE_SDK/examples/helloworld # 进入 DPDK 中的 helloworld 例子