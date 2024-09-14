

namespace tinyptr {

struct DerefTabEntry {
   public:
    Entry(int key, int value) : key_(key), value_(value) {}

    int getKey() const { return key_; }

    int getValue() const { return value_; }

   private:
    int key_;
    int value_;
};
}  // namespace tinyptr
