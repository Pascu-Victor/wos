#ifndef WOSDBG_H
#define WOSDBG_H

#include <qcontainerfwd.h>
#include <qtextedit.h>
#include <qtmetamacros.h>
#include <qurl.h>

#include <cstddef>
#include <cstdint>

#include "coredump_parser.h"
#include "elf_symbol_resolver.h"

// Forward declarations to avoid including BFD in header
using bfd = struct bfd;
using asymbol = struct bfd_symbol;
using csh = size_t;  // Forward declare capstone handle

#include <QtCore/QJsonObject>
#include <QtCore/QObject>
#include <QtCore/QProcess>
#include <QtCore/QRegularExpression>
#include <QtCore/QTemporaryDir>
#include <QtCore/QThread>
#include <QtCore/QTimer>
#include <QtGui/QFont>
#include <QtGui/QSyntaxHighlighter>
#include <QtGui/QTextDocument>
#include <QtWidgets/QCheckBox>
#include <QtWidgets/QComboBox>
#include <QtWidgets/QHBoxLayout>
#include <QtWidgets/QHeaderView>
#include <QtWidgets/QLabel>
#include <QtWidgets/QLineEdit>
#include <QtWidgets/QMainWindow>
#include <QtWidgets/QProgressBar>
#include <QtWidgets/QPushButton>
#include <QtWidgets/QScrollBar>
#include <QtWidgets/QSplitter>
#include <QtWidgets/QStyledItemDelegate>
#include <QtWidgets/QTableWidget>
#include <QtWidgets/QTextBrowser>
#include <QtWidgets/QToolBar>
#include <QtWidgets/QTreeWidget>
#include <QtWidgets/QVBoxLayout>
#include <memory>
#include <string>
#include <vector>

// Forward declarations
class LogProcessor;
class CapstoneDisassembler;
class SyntaxHighlighter;
class SyntaxHighlightDelegate;
class VirtualTableView;
class VirtualTableModel;
class LogClient;

// Coredump panel forward declarations
class CoredumpBrowser;
class CoredumpRegisterPanel;
class CoredumpSegmentPanel;
class CoredumpMemoryPanel;
class CoredumpElfPanel;
class CoredumpDisasmPanel;

#include "log_entry.h"

class QemuLogViewer : public QMainWindow {
    Q_OBJECT

   public:
    QemuLogViewer(LogClient* client, QWidget* parent = nullptr);
    ~QemuLogViewer();

   private slots:
    void on_file_selected(const QString& filename);
    void on_file_list_received(const QStringList& files);
    void on_config_received();
    void on_file_ready(int total_lines);
    void on_data_received(int start_line, int count);
    void on_search_results(const std::vector<int>& matches);
    void on_interrupts_received(const std::vector<LogEntry>& interrupts);
    void on_filter_applied(int total_lines);
    void on_row_for_line_received(int row);

    void on_search_text_changed();
    void perform_debounced_search();  // Actual search after debounce
    void on_search_next();
    void on_search_previous();
    void on_regex_toggled(bool enabled);
    void on_hide_structural_toggled(bool enabled);
    void on_navigation_text_changed();
    // void onProcessingComplete(); // Removed as it is legacy
    void on_progress_update(int percentage);
    void on_table_selection_changed();
    void on_hex_view_selection_changed();
    void on_table_cell_clicked(int row, int column);
    static void sync_scroll_bars(int value);
    void on_interrupt_filter_changed(const QString& text);
    void on_interrupt_panel_activated(QTreeWidgetItem* item, int column);
    void on_interrupt_next();
    void on_interrupt_previous();
    static void on_interrupt_toggle_fold(QTreeWidgetItem* item, int column);
    void on_mcp_toggle();
    void on_mcp_server_status(bool running, const QString& endpoint, const QString& message);

   private:
    // UI Components
    QToolBar* toolbar;
    QComboBox* file_selector;
    QPushButton* refresh_files_btn;
    QLineEdit* search_edit;
    QCheckBox* regex_checkbox;
    QCheckBox* hide_structural_checkbox;
    QPushButton* search_next_btn;
    QPushButton* search_prev_btn;
    QLineEdit* navigation_edit;
    QProgressBar* progress_bar;
    QLabel* status_label;
    QComboBox* interrupt_filter_combo;  // Dropdown to filter interrupts
    QPushButton* interrupt_prev_btn;
    QPushButton* interrupt_next_btn;
    QCheckBox* only_interrupts_checkbox;
    QPushButton* mcp_toggle_btn;

    // Main content
    QTableWidget* legacy_log_table;          // Keep for compatibility during transition
    VirtualTableView* log_table;             // New virtual table
    VirtualTableModel* virtual_table_model;  // Model for virtual table
    QTextEdit* hex_view;
    QTextEdit* disassembly_view;
    QTextBrowser* details_pane;

    // Dock widgets for main content panels
    QDockWidget* interrupts_dock = nullptr;
    QDockWidget* hex_dock = nullptr;
    QDockWidget* disassembly_dock = nullptr;
    QDockWidget* details_dock = nullptr;
    QDockWidget* browser_dock = nullptr;  // Coredump browser dock (stored for tabification)

    // Search functionality
    std::vector<int> search_matches;
    int current_search_index;
    int pre_search_position;  // Store position before search started
    bool search_active;
    QRegularExpression search_regex;
    QTimer* search_debounce_timer;  // Debounce search input

    // Shadow search structure for fast searching
    struct SearchableRow {
        QString combined_text;  // All columns concatenated
        int original_row_index;
    };
    std::vector<SearchableRow> searchable_rows;

    // Data
    LogClient* client;
    bool mcp_running = false;
    QString mcp_endpoint;
    std::unique_ptr<CapstoneDisassembler> disassembler;
    SyntaxHighlighter* disassembly_highlighter;
    SyntaxHighlighter* details_highlighter;
    SyntaxHighlightDelegate* table_delegate;
    QTreeWidget* interrupts_panel;  // Left-side panel listing interrupts

    // Interrupt navigation state
    std::string current_selected_interrupt;  // raw interrupt number string
    int current_interrupt_index;             // index within visible occurrences

    // Performance optimization members
    std::vector<QString> string_buffers;              // Pre-allocated string buffers
    static constexpr size_t STRING_BUFFER_SIZE = 50;  // Number of string buffers
    size_t next_string_buffer;

    // Methods
    void setup_dark_theme();
    void apply_theme(const QString& theme_name);
    static QString get_dark_theme_css();
    static QString get_light_theme_css();
    static QString get_high_contrast_theme_css();
    void setup_ui();
    void setup_toolbar();
    void setup_main_content();
    void setup_table();
    void connect_signals();
    void load_log_files();
    void populate_table();             // Optimized version
    void populate_interrupt_filter();  // Populate interrupt dropdown
    void perform_search();
    void perform_search_optimized();  // Optimized version
    void highlight_search_matches();
    void jump_to_address(uint64_t address);
    void jump_to_line(int line_number);
    void update_hex_view(const LogEntry& entry);
    void update_disassembly_view(const LogEntry& entry);
    void update_details_pane(int row);
    void scroll_to_row(int row);
    void scroll_to_row_for_search(int row);  // Search-specific scrolling that ignores modifier keys
    static bool is_address_input(const QString& text);
    static QString format_address(const std::string& addr);
    static QString format_function(const std::string& func);
    static QString format_hex_bytes(const std::string& bytes);
    QString format_assembly(const std::string& assembly);
    static QString extract_file_info(const std::string& func);                            // Extract file:line:column from function
    static QString get_source_code_snippet(const QString& source_file, int source_line);  // Get source code snippet
    static QColor get_entry_type_color(EntryType type);                                   // Get color for entry type

    // Event handling
    bool eventFilter(QObject* obj, QEvent* event) override;  // NOLINT
    void cancel_search();                                    // Cancel search and return to original position
    void on_details_pane_link_clicked(const QUrl& url);      // Handle clicks on VS Code links in details pane

    // Performance optimization methods
    void initialize_performance_optimizations();
    void build_lookup_maps();
    void build_searchable_rows();  // Build shadow search structure
    static QTableWidgetItem* get_pooled_item();
    static void return_item_to_pool(QTableWidgetItem* item);
    QString& get_string_buffer();
    static void pre_allocate_table_items(int row_count);
    static void batch_update_table(const std::vector<const LogEntry*>& entries);
    void build_interrupt_panel();
    [[nodiscard]] static int find_next_iret_line(int start_line_number);  // Find the next iret instruction after a given line

    // ----- Coredump integration -----
    void setup_coredump_panels();  // Create and wire all coredump dock widgets
    void open_coredump(const QString& file_path);
    void close_coredump();
    void resolve_symbols_for_coredump();              // Auto-resolve from filename + config
    void on_coredump_address_clicked(uint64_t addr);  // Navigate log/memory to address
    void browse_coredump_directory();                 // Toolbar: choose coredump directory
    void extract_coredumps();                         // Toolbar: run extract script
    void refresh_coredumps();                         // Toolbar: rescan directory

    // Coredump dock panels
    CoredumpBrowser* coredump_browser = nullptr;
    CoredumpRegisterPanel* register_panel = nullptr;
    CoredumpSegmentPanel* segment_panel = nullptr;
    CoredumpMemoryPanel* memory_panel = nullptr;
    CoredumpElfPanel* elf_panel = nullptr;
    CoredumpDisasmPanel* disasm_panel = nullptr;

    // Coredump state
    std::unique_ptr<wosdbg::CoreDump> current_core_dump;
    std::unique_ptr<wosdbg::SymbolTable> core_dump_symtab;
    std::unique_ptr<wosdbg::SectionMap> core_dump_sections;
    // Additional symbol sources (kernel, embedded ELF, etc.)
    std::unique_ptr<wosdbg::SymbolTable> embedded_symtab;
    std::unique_ptr<wosdbg::SectionMap> embedded_sections;
    std::unique_ptr<wosdbg::SymbolTable> kernel_symtab;
    std::unique_ptr<wosdbg::SectionMap> kernel_sections;
};

#endif  // WOSDBG_H
