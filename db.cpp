#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <cstddef>

class InputBuffer {
public:
    char* buffer;
    size_t buffer_length;
    ssize_t input_length;

    InputBuffer(): buffer{nullptr}, buffer_length{0}, input_length{0} {}
    ~InputBuffer() {
        delete[] buffer;
    }
};

class Database {
public:
    void print_prompt() {
        std::cout << "db > ";
    }
    void read_input(InputBuffer* input_buffer) {
        ssize_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);
        if (bytes_read <= 0) {
            std::cout << "Error reading input\n";
            exit(EXIT_FAILURE);
        }
        input_buffer->input_length = bytes_read - 1;
        input_buffer->buffer[bytes_read - 1] = '\0';
    }
    void close_input_buffer(InputBuffer* input_buffer) {
        delete input_buffer;
    }
    void run() {
        InputBuffer* input_buffer = new InputBuffer{};
        while (true) {
            print_prompt();
            read_input(input_buffer);
            if (strcmp(input_buffer->buffer, ".exit") == 0) {
                close_input_buffer(input_buffer);
                exit(EXIT_SUCCESS);
            } else {
                std::cout << "Unrecognized command " << input_buffer->buffer << std::endl;
            }
        }
    }
};

int main() {
    Database db{};
    db.run();
    return 0;
}