#define GNU_SOURCE

#include <dlfcn.h>
#include <cstdio>
#include <ucontext.h>
#include <map>
#include <list>
#include <queue>
#include <memory>
#include <chrono>
#include <iostream>
#include <thread>
#include <cerrno>
#include <cstring>
#include <sys/poll.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <sys/socket.h>
#include <mutex>

enum class CoroutineState
{
    READY,
    WAITING,
    FINISHED
};

class Coroutine
{
public:
    using CoroutineFunc = void(*)(void*);

private:
    ucontext_t context_{}; //这里面的栈和外面的栈不一样
    CoroutineState state_ = CoroutineState::READY;
    std::unique_ptr<char[]> stack_;
    size_t stack_size_;
    int waiting_fd_ = -1; // 当前等待的fd (-1表示没有等待)
    CoroutineFunc func_; // 协程要执行的函数
    void* arg_; // 函数参数

public:
    // 构造函数
    Coroutine(CoroutineFunc func, void* arg, size_t stack_size = 8192);

    // 禁止拷贝，允许移动
    Coroutine(const Coroutine&) = delete;
    Coroutine& operator=(const Coroutine&) = delete;
    Coroutine(Coroutine&&) = default;
    Coroutine& operator=(Coroutine&&) = default;

    ~Coroutine() = default;

    ucontext_t& context() { return context_; }
    [[nodiscard]] const ucontext_t& context() const { return context_; }
    [[nodiscard]] CoroutineState state() const { return state_; }
    [[nodiscard]] int waiting_fd() const { return waiting_fd_; }

    void set_state(const CoroutineState state) { state_ = state; }
    void set_waiting_fd(const int fd) { waiting_fd_ = fd; }
    void clear_waiting_fd() { waiting_fd_ = -1; }

    void execute() const { func_(arg_); }
};

class Scheduler
{
    std::chrono::steady_clock::time_point idle_start_time_;
    bool is_idle_ = false;

    // 清理计数
    int schedule_count_ = 0;
    int finished_coroutines_count_ = 0;

    static thread_local std::unique_ptr<Scheduler> instance_;

    std::list<std::unique_ptr<Coroutine>> all_coroutines_; // 所有协程
    std::queue<Coroutine*> ready_queue_; // 就绪队列
    Coroutine* current_coroutine_ = nullptr; // 当前运行的协程
    ucontext_t main_context_{}; // 主上下文（调度器的上下文）
    int epoll_fd_ = -1; // epoll 文件描述符
    std::map<int, Coroutine*> fd_to_coroutine_; // 存储的是 WAITING 协程
    bool should_continue_ = true;

    Scheduler();

public:
    ~Scheduler();

    static Scheduler& instance();

    Coroutine* create_coroutine(Coroutine::CoroutineFunc func, void* arg);
    void yield() const;
    void wait_for_read(int fd);

    void run();

    ucontext_t& get_main_context();

    [[nodiscard]] bool in_coroutine() const { return current_coroutine_ != nullptr; }
    [[nodiscard]] bool has_waiting_coroutines() const { return !fd_to_coroutine_.empty(); }

    static void wrapper_function(Coroutine* coroutine);

private:
    void check_io_events();
    void schedule_once();
    void run_one_coroutine();
    void handle_coroutine_return();
    void handle_idle_state();
    void cleanup_finished_coroutines();
    void reset_idle_state();
};

void Scheduler::wait_for_read(const int fd)
{
    if (!current_coroutine_)
    {
        std::cerr << "警告：不在协程中调用wait_for_read" << std::endl;
        return;
    }

    if (current_coroutine_->waiting_fd() != -1)
    {
        std::cerr << "警告：协程已在等待fd " << current_coroutine_->waiting_fd() << std::endl;
        return;
    }

    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = fd; // 存储fd？

    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) == -1)
    {
        if (errno == EEXIST)
        {
            epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev);
        }
        else
        {
            perror("epoll_ctl failed in wait_for_read");
            return;
        }
    }

    current_coroutine_->set_state(CoroutineState::WAITING);

    current_coroutine_->set_waiting_fd(fd);

    fd_to_coroutine_[fd] = current_coroutine_;

    swapcontext(&current_coroutine_->context(), &main_context_);
}

Scheduler::~Scheduler()
{
    if (epoll_fd_ >= 0)
    {
        close(epoll_fd_);
    }
    std::cout << "协程调度器析构完成" << std::endl;
}

void Scheduler::check_io_events()
{
    epoll_event events[64]{};

    const int nfds = epoll_wait(epoll_fd_, events, 64, 100);

    if (nfds < 0)
    {
        if (errno != EINTR)
        {
            perror("epoll_wait failed");
        }
        return;
    }

    for (int i{0}; i < nfds; ++i)
    {
        int ready_fd = events[i].data.fd;
        if (Coroutine* waiting_co = fd_to_coroutine_[ready_fd]; waiting_co && waiting_co->state() ==
            CoroutineState::WAITING)
        {
            waiting_co->set_state(CoroutineState::READY);

            waiting_co->clear_waiting_fd(); // 设为-1

            fd_to_coroutine_.erase(ready_fd);

            epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, ready_fd, nullptr);

            ready_queue_.push(waiting_co);
        }
    }
}

void Scheduler::yield() const
{
    if (!current_coroutine_)
    {
        std::cerr << "警告：不在协程中调用yield" << std::endl;
        return;
    }
    current_coroutine_->set_state(CoroutineState::READY);
    swapcontext(&current_coroutine_->context(), &main_context_);
}

void Scheduler::reset_idle_state()
{
    is_idle_ = false;
}

Scheduler& Scheduler::instance()
{
    if (!instance_)
    {
        instance_ = std::unique_ptr<Scheduler>(new Scheduler());
    }
    return *instance_;
}

void Scheduler::handle_coroutine_return()
{
    switch (current_coroutine_->state())
    {
    case CoroutineState::READY:
        ready_queue_.push(current_coroutine_);
        break;

    case CoroutineState::WAITING:

        break;

    case CoroutineState::FINISHED:

        break;
    }
}

void Scheduler::run_one_coroutine()
{
    if (ready_queue_.empty()) return;

    Coroutine* next = ready_queue_.front();
    ready_queue_.pop();
    current_coroutine_ = next;

    swapcontext(&main_context_, &next->context());

    if (current_coroutine_)
    {
        handle_coroutine_return();
    }

    current_coroutine_ = nullptr;
}

void Scheduler::handle_idle_state()
{
    const auto now = std::chrono::steady_clock::now();

    if (!is_idle_)
    {
        // 第一次进入空闲状态
        is_idle_ = true;
        idle_start_time_ = now;
        std::cout << "调度器进入空闲状态，开始计时..." << std::endl;
    }

    const auto idle_duration = std::chrono::duration_cast<std::chrono::seconds>(
        now - idle_start_time_).count();

    if (idle_duration >= 5)
    {
        std::cout << "空闲 " << idle_duration << " 秒，调度器退出" << std::endl;
        should_continue_ = false;
    }
    else
    {
        std::cout << "空闲中... (" << idle_duration << "s)" << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

void Scheduler::cleanup_finished_coroutines()
{
    int cleaned_count = 0;

    auto it = all_coroutines_.begin();
    while (it != all_coroutines_.end())
    {
        if ((*it)->state() == CoroutineState::FINISHED)
        {
            it = all_coroutines_.erase(it);
            cleaned_count++;
        }
        else
        {
            ++it;
        }
    }

    if (cleaned_count > 0)
    {
        std::cout << "清理了 " << cleaned_count << " 个已完成的协程，"
            << "剩余 " << all_coroutines_.size() << " 个协程" << std::endl;
    }
}

void Scheduler::schedule_once()
{
    if (++schedule_count_ >= 1000)
    {
        cleanup_finished_coroutines();
        schedule_count_ = 0;
    }

    if (!ready_queue_.empty())
    {
        run_one_coroutine();
        reset_idle_state();
        return;
    }

    if (has_waiting_coroutines())
    {
        check_io_events();
        reset_idle_state();
        return;
    }

    handle_idle_state();
}

void Scheduler::run()
{
    getcontext(&main_context_);
    while (should_continue_)
    {
        schedule_once();
    }
}

class HookSystem
{
    static bool initialized_;

public:
    static void initialize();
    static bool is_initialized() { return initialized_; }
};

typedef ssize_t (*read_t)(int fd, void* buf, size_t count);
typedef ssize_t (*write_t)(int fd, const void* buf, size_t count);

static read_t original_read = nullptr;
static write_t original_write = nullptr;

void init_hooks()
{
    static std::once_flag init_flag;
    std::call_once(init_flag, []
    {
        original_read = (read_t)dlsym(RTLD_NEXT, "read");
        original_write = (write_t)dlsym(RTLD_NEXT, "write");
        std::cout << "Hook函数指针初始化完成" << std::endl;
    });
}

// Hook read函数
extern "C" ssize_t read(const int fd, void* buf, const size_t count)
{
    init_hooks();

    auto& scheduler = Scheduler::instance();
    if (!scheduler.in_coroutine())
    {
        return original_read(fd, buf, count);
    }

    pollfd pfd = {fd, POLLIN, 0};

    if (const int result = poll(&pfd, 1, 0); result <= 0)
    {
        std::cout << "协程等待fd " << fd << " 可读..." << std::endl;
        scheduler.wait_for_read(fd);
        std::cout << "协程被唤醒，fd " << fd << " 可读了！" << std::endl;
    }

    const ssize_t ret = original_read(fd, buf, count);
    std::cout << "read返回: " << ret << " bytes" << std::endl;
    return ret;
}

extern "C" ssize_t write(const int fd, const void* buf, const size_t count)
{
    init_hooks();
    std::cout << "write调用: " << count << " bytes to fd " << fd << std::endl;
    return original_write(fd, buf, count);
}

void HookSystem::initialize()
{
    if (!initialized_)
    {
        std::cout << "Hook系统初始化完成" << std::endl;
        initialized_ = true;
    }
}

ucontext_t& Scheduler::get_main_context()
{
    return main_context_;
}

void Scheduler::wrapper_function(Coroutine* coroutine)
{
    try
    {
        coroutine->execute();
    }
    catch (const std::exception& e)
    {
        std::cerr << "协程异常: " << e.what() << std::endl;
    } catch (...)
    {
        std::cerr << "协程未知异常" << std::endl;
    }
    coroutine->set_state(CoroutineState::FINISHED);

    Scheduler& scheduler = instance();
    swapcontext(&coroutine->context(), &scheduler.get_main_context());
}

Coroutine::Coroutine(const CoroutineFunc func, void* arg, const size_t stack_size)
    : stack_size_(stack_size), func_(func), arg_(arg)
{
    stack_ = std::make_unique<char[]>(stack_size);

    getcontext(&context_);
    context_.uc_stack.ss_sp = stack_.get();
    context_.uc_stack.ss_size = stack_size;
    context_.uc_link = nullptr;

    makecontext(&context_, reinterpret_cast<void(*)()>(Scheduler::wrapper_function), 1, this);
    state_ = CoroutineState::READY;
    waiting_fd_ = -1;
}

Scheduler::Scheduler()
{
    epoll_fd_ = epoll_create1(0);
    if (epoll_fd_ < 0)
    {
        std::cerr << "错误: 无法创建epoll文件描述符: " << strerror(errno) << std::endl;
        std::cerr << "协程调度器初始化失败，程序退出" << std::endl;
        exit(1);
    }

    std::cout << "协程调度器初始化成功" << std::endl;
}

Coroutine* Scheduler::create_coroutine(Coroutine::CoroutineFunc func, void* arg)
{
    all_coroutines_.push_back(std::make_unique<Coroutine>(func, arg));

    Coroutine* co_ptr = all_coroutines_.back().get();

    ready_queue_.push(co_ptr);

    return co_ptr;
}

thread_local std::unique_ptr<Scheduler> Scheduler::instance_;
bool HookSystem::initialized_ = false;

// //多协程调度
// int main()
// {
//     HookSystem::initialize();
//     auto& scheduler = Scheduler::instance();
//
//     // 使用静态数组，避免动态内存分配
//     static int args[] = {1, 2, 3};
//
//     for (int& i : args)
//     {
//         scheduler.create_coroutine([](void* arg)
//         {
//             const auto id = static_cast<int*>(arg);
//             printf("协程 %d 开始\n", *id);
//
//             for (int j = 0; j < 3; j++)
//             {
//                 printf("协程 %d 工作 %d\n", *id, j + 1);
//                 Scheduler::instance().yield();
//             }
//
//             printf("协程 %d 结束\n", *id);
//             // 不需要delete，因为是静态数组
//         }, &i);
//     }
//
//     scheduler.run();
//     return 0;
// }


// //异步IO测试
// int main()
// {
//     HookSystem::initialize();
//     auto& scheduler = Scheduler::instance();
//
//     // 创建一个socket对测试IO等待
//     int sockpair[2];
//     socketpair(AF_UNIX, SOCK_STREAM, 0, sockpair);
//
//     scheduler.create_coroutine([](void* arg)
//     {
//         const auto read_fd = static_cast<int*>(arg);
//         char buffer[64];
//
//         printf("协程准备读取socket %d\n", *read_fd);
//
//         // 这里会触发wait_for_read，因为socket暂时没有数据
//
//         if (const ssize_t n = read(*read_fd, buffer, sizeof(buffer)); n > 0)
//         {
//             buffer[n] = '\0';
//             printf("协程读取到: %s\n", buffer);
//         }
//     }, &sockpair[0]);
//
//     // 创建另一个协程写入数据
//     scheduler.create_coroutine([](void* arg)
//     {
//         const auto write_fd = static_cast<int*>(arg);
//
//         // 延迟一下再写入
//         for (int i = 0; i < 3; i++)
//         {
//             Scheduler::instance().yield();
//         }
//
//         printf("协程写入数据到socket %d\n", *write_fd);
//         write(*write_fd, "Hello from coroutine!", 21);
//         close(*write_fd);
//     }, &sockpair[1]);
//
//     scheduler.run();
//
//     close(sockpair[0]);
//     return 0;
// }
