#ifndef QEMU_LOG_VIEWER_H
#define QEMU_LOG_VIEWER_H

#include <cstddef>
#include <cstdint>

// Forward declarations to avoid including BFD in header
typedef struct bfd bfd;
typedef struct bfd_symbol asymbol;
typedef size_t csh;  // Forward declare capstone handle

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
#include <QtWidgets/QTextEdit>
#include <QtWidgets/QToolBar>
#include <QtWidgets/QTreeWidget>
#include <QtWidgets/QVBoxLayout>
#include <memory>
#include <regex>
#include <string>
#include <unordered_map>
#include <vector>

// Forward declarations
class LogProcessor;
class CapstoneDisassembler;
class SyntaxHighlighter;
class SyntaxHighlightDelegate;

enum class EntryType { INSTRUCTION, INTERRUPT, REGISTER, BLOCK, SEPARATOR, OTHER };

struct LogEntry {
    int lineNumber;
    EntryType type;
    std::string address;
    std::string function;
    std::string hexBytes;
    std::string assembly;
    std::string originalLine;
    uint64_t addressValue;

    // For grouped entries (like interrupts)
    bool isExpanded;
    std::vector<LogEntry> childEntries;
    bool isChild;

    // Interrupt-specific fields
    std::string interruptNumber;
    std::string cpuStateInfo;

    LogEntry() : lineNumber(0), type(EntryType::OTHER), addressValue(0), isExpanded(false), isChild(false) {}
};

class LogProcessor : public QObject {
    Q_OBJECT

   public:
    LogProcessor(const QString& filename, QObject* parent = nullptr);
    void startProcessing();
    std::vector<LogEntry> getEntries() const { return entries; }

   signals:
    void progressUpdate(int percentage);
    void processingComplete();
    void errorOccurred(const QString& error);

   private slots:
    void onWorkerFinished(int exitCode, QProcess::ExitStatus exitStatus);
    void onWorkerError(QProcess::ProcessError error);

   private:
    QString filename;
    std::vector<LogEntry> entries;
    QTemporaryDir tempDir;
    std::vector<QProcess*> workers;
    int completedWorkers;
    int totalWorkers;

    void splitFileIntoChunks();
    void startWorkerProcesses();
    void mergeResults();
    LogEntry parseLogEntryFromJson(const QJsonObject& json);
};

class QemuLogViewer : public QMainWindow {
    Q_OBJECT

   public:
    QemuLogViewer(QWidget* parent = nullptr);
    ~QemuLogViewer();

   private slots:
    void onFileSelected(const QString& filename);
    void onSearchTextChanged();
    void performDebouncedSearch();  // Actual search after debounce
    void onSearchNext();
    void onSearchPrevious();
    void onRegexToggled(bool enabled);
    void onHideStructuralToggled(bool enabled);
    void onNavigationTextChanged();
    void onProcessingComplete();
    void onProgressUpdate(int percentage);
    void onTableSelectionChanged();
    void onHexViewSelectionChanged();
    void onTableCellClicked(int row, int column);
    void syncScrollBars(int value);
    void onInterruptFilterChanged(const QString& text);
    void onInterruptPanelActivated(QTreeWidgetItem* item, int column);
    void onInterruptNext();
    void onInterruptPrevious();

   private:
    // UI Components
    QToolBar* toolbar;
    QComboBox* fileSelector;
    QLineEdit* searchEdit;
    QCheckBox* regexCheckbox;
    QCheckBox* hideStructuralCheckbox;
    QPushButton* searchNextBtn;
    QPushButton* searchPrevBtn;
    QLineEdit* navigationEdit;
    QProgressBar* progressBar;
    QLabel* statusLabel;
    QComboBox* interruptFilterCombo;  // Dropdown to filter interrupts
    QPushButton* interruptPrevBtn;
    QPushButton* interruptNextBtn;
    QCheckBox* onlyInterruptsCheckbox;

    // Main content
    QSplitter* mainSplitter;
    QTableWidget* logTable;
    QTextEdit* hexView;
    QTextEdit* disassemblyView;
    QTextEdit* detailsPane;

    // Search functionality
    std::vector<int> searchMatches;
    int currentSearchIndex;
    int preSearchPosition;  // Store position before search started
    bool searchActive;
    QRegularExpression searchRegex;
    QTimer* searchDebounceTimer;  // Debounce search input

    // Shadow search structure for fast searching
    struct SearchableRow {
        QString combinedText;  // All columns concatenated
        int originalRowIndex;
    };
    std::vector<SearchableRow> searchableRows;

    // Data
    std::unique_ptr<LogProcessor> processor;
    std::vector<LogEntry> logEntries;
    std::vector<const LogEntry*> visibleEntryPointers;  // Maps table row to actual log entry
    std::unique_ptr<CapstoneDisassembler> disassembler;
    SyntaxHighlighter* disassemblyHighlighter;
    SyntaxHighlighter* detailsHighlighter;
    SyntaxHighlightDelegate* tableDelegate;
    QTreeWidget* interruptsPanel;                            // Left-side panel listing interrupts
    std::unordered_map<size_t, int> entryIndexToVisibleRow;  // map from logEntries index to visible row
    // Interrupt navigation state
    std::string currentSelectedInterrupt;  // raw interrupt number string
    int currentInterruptIndex;             // index within visible occurrences

    // Performance optimization members
    std::vector<QString> stringBuffers;                      // Pre-allocated string buffers
    std::unordered_map<uint64_t, size_t> addressToEntryMap;  // Fast address lookup
    std::unordered_map<int, size_t> lineToEntryMap;          // Fast line lookup
    static constexpr size_t STRING_BUFFER_SIZE = 50;         // Number of string buffers
    size_t nextStringBuffer;

    // Methods
    void setupDarkTheme();
    void applyTheme(const QString& themeName);
    QString getDarkThemeCSS();
    QString getLightThemeCSS();
    QString getHighContrastThemeCSS();
    void setupUI();
    void setupToolbar();
    void setupMainContent();
    void setupTable();
    void connectSignals();
    void loadLogFiles();
    void populateTable();
    void populateTableOptimized();   // Optimized version
    void populateInterruptFilter();  // Populate interrupt dropdown
    void performSearch();
    void performSearchOptimized();  // Optimized version
    void highlightSearchMatches();
    void jumpToAddress(uint64_t address);
    void jumpToLine(int lineNumber);
    void updateHexView(const LogEntry& entry);
    void updateDisassemblyView(const LogEntry& entry);
    void updateDetailsPane(int row);
    void scrollToRow(int row);
    void scrollToRowForSearch(int row);  // Search-specific scrolling that ignores modifier keys
    bool isAddressInput(const QString& text);
    QString formatAddress(const std::string& addr);
    QString formatFunction(const std::string& func);
    QString formatHexBytes(const std::string& bytes);
    QString formatAssembly(const std::string& assembly);
    QString extractFileInfo(const std::string& func);  // Extract file:line:column from function
    QColor getEntryTypeColor(EntryType type);          // Get color for entry type

    // Event handling
    bool eventFilter(QObject* obj, QEvent* event) override;
    void cancelSearch();  // Cancel search and return to original position

    // Performance optimization methods
    void initializePerformanceOptimizations();
    void buildLookupMaps();
    void buildSearchableRows();  // Build shadow search structure
    QTableWidgetItem* getPooledItem();
    void returnItemToPool(QTableWidgetItem* item);
    QString& getStringBuffer();
    void preAllocateTableItems(int rowCount);
    void batchUpdateTable(const std::vector<const LogEntry*>& entries);
    void buildInterruptPanel();
};

#endif  // QEMU_LOG_VIEWER_H
