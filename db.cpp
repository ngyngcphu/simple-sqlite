#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <iostream>
#include <cstddef>
#include <cstdint>

enum MetaCommandResult {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
};

enum PrepareResult {
    PREPARE_SUCCESS,
    PREPARE_NEGATIVE_ID,
    PREPARE_STRING_TOO_LONG,
    PREPARE_SYNTAX_ERROR,
    PREPARE_UNRECOGNIZED_STATEMENT
};

enum StatementType {
    STATEMENT_INSERT,
    STATEMENT_SELECT
};

enum ExecuteResult {
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL
};

enum NodeType {
    NODE_INTERNAL,
    NODE_LEAF
};

#define TABLE_MAX_PAGES 100
const uint32_t PAGE_SIZE = 4096;

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

class Row {
public:
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];

    Row() {
        id = 0;
        username[0] = '\0';
        email[0] = '\0';
    }
    Row(uint32_t id, const char *username, const char *email) {
        this->id = id;
        strncpy(this->username, username, COLUMN_USERNAME_SIZE + 1);
        strncpy(this->email, email, COLUMN_EMAIL_SIZE + 1);
    }
};

#define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

void serialize_row(Row* source, void *destination) {
    memcpy((char *)destination + ID_OFFSET, &(source->id), ID_SIZE);
    strncpy((char *)destination + USERNAME_OFFSET, source->username, USERNAME_SIZE);
    strncpy((char *)destination + EMAIL_OFFSET, source->email, EMAIL_SIZE);
}

void deserialize_row(void *source, Row *destination) {
    memcpy(&(destination->id), (char *)source + ID_OFFSET, ID_SIZE);
    strncpy(destination->username, (char *)source + USERNAME_OFFSET, USERNAME_SIZE);
    strncpy(destination->email, (char *)source + EMAIL_OFFSET, EMAIL_SIZE);
}

class Pager {
public:
    int file_description;
    uint32_t file_length;
    void *pages[TABLE_MAX_PAGES];
    uint32_t num_pages;

    Pager(const char *filename);
    void *get_page(uint32_t page_num);
    void pager_flush(uint32_t page_num);
};

Pager::Pager(const char *filename) {
    file_description = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
    if (file_description < 0) {
        std::cerr << "Error: cannot open file " << filename << std::endl;
        exit(EXIT_FAILURE);
    }
    file_length = lseek(file_description, 0, SEEK_END);
    num_pages = file_length / PAGE_SIZE;
    if (file_length % PAGE_SIZE != 0) {
        std::cerr << "Db file is not a whole number of pages. Corrupt file." << std::endl;
        exit(EXIT_FAILURE);
    }
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; ++i) {
        pages[i] = nullptr;
    }
}

void *Pager::get_page(uint32_t page_num) {
    if (page_num > TABLE_MAX_PAGES) {
        std::cout << "Tried to fetch page number out of bounds. " << page_num << " > "
                    << TABLE_MAX_PAGES << std::endl;
        exit(EXIT_FAILURE);
    }
    if (pages[page_num] == nullptr) {
        void *page = malloc(PAGE_SIZE);
        uint32_t num_pages = file_length / PAGE_SIZE;
        if (file_length % PAGE_SIZE != 0) {
            num_pages++;
        }
        if (page_num <= num_pages) {
            lseek(file_description, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(file_description, page, PAGE_SIZE);
            if (bytes_read == -1) {
                std::cout << "Error reading file: " << errno << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        pages[page_num] = page;
        if (page_num >= num_pages) {
            this->num_pages = page_num + 1;
        }
    }
    return pages[page_num];
}

void Pager::pager_flush(uint32_t page_num) {
    if (pages[page_num] == nullptr) {
        std::cout << "Tried to flush null page\n";
        exit(EXIT_FAILURE);
    }
    off_t offset = lseek(file_description, page_num * PAGE_SIZE, SEEK_SET);
    if (offset == -1) {
        std::cout << "Error seeking: " << errno << std::endl;
        exit(EXIT_FAILURE);
    }
    ssize_t bytes_written = write(file_description, pages[page_num], PAGE_SIZE);
    if (bytes_written == -1) {
        std::cout << "Error writting: " << errno << std::endl;
        exit(EXIT_FAILURE);
    }
}

const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint32_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

class LeafNode {
private:
    void *node;
public:
    LeafNode(void *node) : node{ node } {}
    uint32_t *leaf_node_num_cells() {
        return (uint32_t *)((char *)node + LEAF_NODE_NUM_CELLS_OFFSET);
    }
    void *leaf_node_cell(uint32_t cell_num) {
        return (char *)node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
    }
    uint32_t *leaf_node_key(uint32_t cell_num) {
        return (uint32_t *)leaf_node_cell(cell_num);
    }
    void *leaf_node_value(uint32_t cell_num) {
        return (char *)leaf_node_cell(cell_num) + LEAF_NODE_KEY_SIZE;
    }
    void initialize_leaf_node() {
        *leaf_node_num_cells() = 0;
    }
    void print_leaf_node() {
        uint32_t num_cells = *leaf_node_num_cells();
        std::cout << "leaf (size " << num_cells << ")" << std::endl;
        for (uint32_t i = 0; i < num_cells; ++i) {
            uint32_t key = *leaf_node_key(i);
            std::cout << "  - " << i << " : " << key << std::endl;
        }
    }
};

class Table {
public:
    uint32_t root_page_num;
    Pager *pager;

    Table(const char *filename) {
        pager = new Pager{ filename };
        root_page_num = 0;
        if (pager->num_pages == 0) {
            LeafNode *root_node = new LeafNode { pager->get_page(0) };
            root_node->initialize_leaf_node();
            delete root_node;
        }
    }
    ~Table();
};

Table::~Table() {
    for (uint32_t i = 0; i < pager->num_pages; ++i) {
        if (pager->pages[i] == nullptr) {
            continue;
        }
        pager->pager_flush(i);
        free(pager->pages[i]);
        pager->pages[i] = nullptr;
    }
    int result = close(pager->file_description);
    if (result == -1) {
        std::cout << "Error closing db file.\n";
        exit(EXIT_FAILURE);
    }
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; ++i) {
        void *page = pager->pages[i];
        if (page) {
            free(page);
            pager->pages[i] = nullptr;
        }
    }
    delete pager;
}

class Cursor {
private:
    Table *table;
    uint32_t page_num;
    uint32_t cell_num;
    bool end_of_table;

public:
    Cursor(Table *table, bool option);
    void *cursor_value();
    void cursor_advance();
    void leaf_node_insert(uint32_t key, Row *value);

    friend class Database;
};

Cursor::Cursor(Table *table, bool option) {
    this->table = table;
    page_num = table->root_page_num;
    LeafNode *root_node = new LeafNode{ table->pager->get_page(table->root_page_num) };
    uint32_t num_cells = *root_node->leaf_node_num_cells();
    if (option) {
        cell_num = 0;
        end_of_table = (num_cells == 0);
    } else {
        cell_num = num_cells;
        end_of_table = true;
    }
    delete root_node;
}

void *Cursor::cursor_value() {
    LeafNode *page = new LeafNode{ table->pager->get_page(page_num) };
    void *result = page->leaf_node_value(cell_num);
    delete page;
    return result;
}

void Cursor::cursor_advance() {
    LeafNode *node = new LeafNode{ table->pager->get_page(page_num) };
    cell_num += 1;
    if (cell_num >= *node->leaf_node_num_cells()) {
        end_of_table = true;
    }
    delete node;
}

void Cursor::leaf_node_insert(uint32_t key, Row *value) {
    LeafNode *node = new LeafNode{ table->pager->get_page(page_num) };
    uint32_t num_cells = *node->leaf_node_num_cells();
    if (num_cells >= LEAF_NODE_MAX_CELLS) {
        std::cout << "Need to implement splitting a leaf node.\n";
        exit(EXIT_FAILURE);
    }
    if (cell_num < num_cells) {
        for (uint32_t i = num_cells; i > cell_num; --i) {
            memcpy(node->leaf_node_cell(i), node->leaf_node_cell(i - 1), LEAF_NODE_CELL_SIZE);
        }
    }
    *(node->leaf_node_num_cells()) += 1;
    *(node->leaf_node_key(cell_num)) = key;
    serialize_row(value, node->leaf_node_value(cell_num));
    delete node;
}

class Statement {
public:
    StatementType type;
    Row row_to_insert;
};

class InputBuffer {
public:
    char *buffer;
    size_t buffer_length;
    ssize_t input_length;

    InputBuffer(): buffer{nullptr}, buffer_length{0}, input_length{0} {}
    ~InputBuffer() {
        delete[] buffer;
        buffer = nullptr;
    }
};

class Database {
private:
    Table *table;

public:
    Database(const char *filename) {
        table = new Table{ filename };
    }
    void print_prompt() {
        std::cout << "db > ";
    }
    void read_input(InputBuffer *input_buffer);
    MetaCommandResult do_meta_command(InputBuffer *input_buffer);
    PrepareResult prepare_insert(InputBuffer *input_buffer, Statement *statement);
    PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement);
    ExecuteResult execute_insert(Statement *statement);
    ExecuteResult execute_select(Statement *statement);
    ExecuteResult execute_statement(Statement *statement);
    void run();
};

void Database::read_input(InputBuffer *input_buffer) {
    ssize_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);
    if (bytes_read <= 0) {
        std::cout << "Error reading input\n";
        exit(EXIT_FAILURE);
    }
    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = '\0';
}

MetaCommandResult Database::do_meta_command(InputBuffer *input_buffer) {
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        delete input_buffer;
        delete table;
        input_buffer = nullptr;
        table = nullptr;
        std::cout << "Bye!\n";
        exit(EXIT_SUCCESS);
    } else if (strcmp(input_buffer->buffer, ".btree") == 0) {
        std::cout << "Tree:\n";
        LeafNode *node = new LeafNode{ table->pager->get_page(table->root_page_num) };
        node->print_leaf_node();
        delete node;
        return META_COMMAND_SUCCESS;
    } else if (strcmp(input_buffer->buffer, ".constants") == 0) {
        std::cout << "Constants:" << std::endl;
        std::cout << "ROW_SIZE: " << ROW_SIZE << std::endl;
        std::cout << "COMMON_NODE_HEADER_SIZE: " << COMMON_NODE_HEADER_SIZE << std::endl;
        std::cout << "LEAF_NODE_HEADER_SIZE: " << LEAF_NODE_HEADER_SIZE << std::endl;
        std::cout << "LEAF_NODE_CELL_SIZE: " << LEAF_NODE_CELL_SIZE << std::endl;
        std::cout << "LEAF_NODE_SPACE_FOR_CELLS: " << LEAF_NODE_SPACE_FOR_CELLS << std::endl;
        std::cout << "LEAF_NODE_MAX_CELLS: " << LEAF_NODE_MAX_CELLS << std::endl;
        return META_COMMAND_SUCCESS;
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult Database::prepare_insert(InputBuffer *input_buffer, Statement *statement) {
    statement->type = STATEMENT_INSERT;
    char *keyword = strtok(input_buffer->buffer, " ");
    char *id_string = strtok(NULL, " ");
    char *username = strtok(NULL, " ");
    char *email = strtok(NULL, " ");
    if (id_string == NULL || username == NULL || email == NULL) {
        return PREPARE_SYNTAX_ERROR;
    }
    int id = strtol(id_string, (char **)NULL, 10);
    if (id < 0) {
        return PREPARE_NEGATIVE_ID;
    }
    if (strlen(username) > COLUMN_USERNAME_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }
    if (strlen(email) > COLUMN_EMAIL_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }
    statement->row_to_insert = Row(id, username, email);
    return PREPARE_SUCCESS;
}

PrepareResult Database::prepare_statement(InputBuffer *input_buffer, Statement *statement) {
    if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
        return prepare_insert(input_buffer, statement);
    } else if (strncmp(input_buffer->buffer, "select", 6) == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    } else {
        return PREPARE_UNRECOGNIZED_STATEMENT;
    }
}

ExecuteResult Database::execute_insert(Statement *statement) {
    LeafNode *node = new LeafNode{ table->pager->get_page(table->root_page_num) };
    if (*node->leaf_node_num_cells() >= LEAF_NODE_MAX_CELLS) {
        delete node;
        return EXECUTE_TABLE_FULL;
    }
    Cursor *cursor = new Cursor{table, false};
    cursor->leaf_node_insert(statement->row_to_insert.id, &(statement->row_to_insert));
    delete cursor;
    return EXECUTE_SUCCESS;
}

ExecuteResult Database::execute_select(Statement *statement) {
    Row *row = new Row{};
    Cursor *cursor = new Cursor{ table, true };
    while (!(cursor->end_of_table)) {
        deserialize_row(cursor->cursor_value(), row);
        std::cout << "(" << row->id << ", " << row->username << ", " << row->email << ")" << std::endl;
        cursor->cursor_advance();
    }
    delete row;
    delete cursor;
    row = nullptr;
    cursor = nullptr;
    return EXECUTE_SUCCESS;
}

ExecuteResult Database::execute_statement(Statement *statement) {
    if (statement->type == STATEMENT_INSERT) {
        return execute_insert(statement);
    } else {
        return execute_select(statement);
    }
}

void Database::run() {
    InputBuffer *input_buffer = new InputBuffer{};
    Statement *statement = new Statement{};

    while (true) {
        print_prompt();
        read_input(input_buffer);
        if (input_buffer->buffer[0] == '.') {
            switch (do_meta_command(input_buffer)) {
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    std::cout << "Unrecognized command " << input_buffer->buffer << std::endl;
                    continue;
            }
        }
        switch (prepare_statement(input_buffer, statement)) {
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_NEGATIVE_ID):
                std::cout << "ID must be positive.\n";
                continue;
            case (PREPARE_STRING_TOO_LONG):
                std::cout << "String is too long.\n";
                continue;
            case (PREPARE_SYNTAX_ERROR):
                std::cout << "Syntax error. Could not parse statement.\n";
                continue;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                std::cout << "Unrecognized keyword at start of " << "'" << input_buffer->buffer << "'." << std::endl;
                continue;
        }
        switch (execute_statement(statement)) {
            case (EXECUTE_SUCCESS):
                std::cout << "Executed.\n";
                break;
            case (EXECUTE_TABLE_FULL):
                std::cout << "Error: Table full.\n";
                break;
        }
    }
}

int main(int argc, const char *argv[]) {
    if (argc < 2) {
        std::cout << "Must supply a database filename." << std::endl;
        exit(EXIT_FAILURE);
    }
    Database db{ argv[1] };
    db.run();
    return 0;
}