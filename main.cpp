#include <thread>
#include <mutex>
#include <vector>
#include <queue>
#include <condition_variable>
#include <functional>
#include <iostream>

// REALIZED TASKS 2 & 3

struct task_t {
  void operator()();
};

static std::mutex cout_mx;

void task_t::operator()() {
  using namespace std::chrono_literals;
  std::this_thread::sleep_for(200ms);
  std::lock_guard<std::mutex> lock(cout_mx); //osyncstream нет на macOS :(((
  std::cout << "!\n";
}

struct th_pool_t
{
    using f_t = std::function< void(void) >;
    explicit th_pool_t(unsigned int k);
    ~th_pool_t();
    void submit(f_t f);
    void wait();
    void pause();
    void resume();
  private:
    void execute();
    std::queue< f_t > tasks_;
    size_t count_tasks = 0;
    std::condition_variable cw_;
    std::mutex mx_;
    std::condition_variable cd_;
    bool stop_{false};
    bool paused_{false};
    std::vector< std::thread > ths_;
};

void th_pool_t::submit(f_t f)
{
  mx_.lock();
  tasks_.push(std::move(f));
  count_tasks++;
  mx_.unlock();
  cd_.notify_one();
}

th_pool_t::th_pool_t(unsigned int k)
{
  auto thf = [this]() { execute(); };
  for (; k; --k) ths_.emplace_back(thf);
}

th_pool_t::~th_pool_t()
{
  mx_.lock();
  stop_ = true;
  paused_ = false;
  mx_.unlock();
  cd_.notify_all();
  for (auto && th: ths_) th.join();
}

void th_pool_t::execute() {
  std::unique_lock<std::mutex> lck(mx_);
  do {
    auto proceed = [&]() {
      return (!tasks_.empty() && !paused_) || stop_;
    };
    cd_.wait(lck, proceed);
    if (tasks_.empty()) continue;
    auto task = std::move(tasks_.front());
    tasks_.pop();
    lck.unlock();
    try { task(); } catch (...) {}
    lck.lock();
    count_tasks--;
    if (count_tasks == 0)
    {
      cw_.notify_all();
    }
  } while (!stop_ || !tasks_.empty());
}

void th_pool_t::wait()
{
  std::unique_lock<std::mutex> lck(mx_);
  auto proceed = [&]() {
    return count_tasks == 0;
  };
  cw_.wait(lck, proceed);
}

void th_pool_t::pause()
{
  mx_.lock();
  paused_ = true;
  mx_.unlock();
}

void th_pool_t::resume()
{
  mx_.lock();
  paused_ = false;
  mx_.unlock();
  cd_.notify_all();
}

int main()
{
  th_pool_t pool(5);
  for (size_t i = 0; i < 10; ++i)
  {
    pool.submit(task_t{});
    if (i == 5)
    {
      pool.wait();
      pool.pause();
      std::cout << "paused\n";
      std::this_thread::sleep_for(std::chrono::seconds(1));
      pool.resume();
      std::cout << "resumed\n";
    }
  }
  pool.wait();
  std::cout << "123\n";
}
