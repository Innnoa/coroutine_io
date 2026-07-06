# coroutine_io

基于 ucontext 的 C++ 用户态协程库，支持协程调度和 Hook 系统调用实现异步 I/O。

## 功能

- **用户态协程**：基于 `ucontext` 实现协程创建、切换与生命周期管理
- **协程调度器**：Round-robin 调度，自动检测空闲并退出
- **I/O Hook**：通过 `dlsym` 劫持 `read`/`write` 系统调用，配合 epoll 实现非阻塞 I/O
- **epoll 集成**：协程在 I/O 等待时自动挂起，数据就绪后唤醒恢复执行

## 构建

```bash
cmake -S . -B build
cmake --build build
```

## 运行

```bash
./build/coroutine_io
```

输出包含两个演示：

1. **多协程调度** — 3 个协程通过 `yield()` 交替执行
2. **异步 I/O** — 通过 socketpair 演示协程挂起等待数据、被写入方唤醒并读取

## 文件结构

```
coroutine_io/
├── CMakeLists.txt   # CMake 构建配置
├── main.cpp         # 协程核心实现 + 演示入口
├── LICENSE          # MIT License
└── README.md        # 本文件
```
