#ifndef QEMU_LOG_VIEWER_H
#define QEMU_LOG_VIEWER_H

#include <qtmetamacros.h>

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
#include <QtWidgets/QTextBrowser>
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
class VirtualTableView;
class VirtualTableModel;
class LogClient;

#include "log_entry.h"

class QemuLogViewer : public QMainWindow {
    Q_OBJECT

   public:
    QemuLogViewer(LogClient* client, QWidget* parent = nullptr);
    ~QemuLogViewer();

   private slots:
    void onFileSelected(const QString& filename);
    void onFileListReceived(const QStringList& files);
    void onConfigReceived();
    void onFileReady(int totalLines);
    void onDataReceived(int startLine, int count);
    void onSearchResults(const std::vector<int>& matches);
    void onInterruptsReceived(const std::vector<LogEntry>& interrupts);
    void onFilterApplied(int totalLines);
    void onRowForLineReceived(int row);

    void onSearchTextChanged();
    void performDebouncedSearch();  // Actual search after debounce
    void onSearchNext();
    void onSearchPrevious();
    void onRegexToggled(bool enabled);
    void onHideStructuralToggled(bool enabled);
    void onNavigationTextChanged();
    // void onProcessingComplete(); // Removed as it is legacy
    void onProgressUpdate(int percentage);
    void onTableSelectionChanged();
    void onHexViewSelectionChanged();
    void onTableCellClicked(int row, int column);
    void syncScrollBars(int value);
    void onInterruptFilterChanged(const QString& text);
    void onInterruptPanelActivated(QTreeWidgetItem* item, int column);
    void onInterruptNext();
    void onInterruptPrevious();
    void onInterruptToggleFold(QTreeWidgetItem* item, int column);

   private:
    // UI Components
    QToolBar* toolbar;
    QComboBox* fileSelector;
    QPushButton* refreshFilesBtn;
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
    QTableWidget* legacyLogTable;          // Keep for compatibility during transition
    VirtualTableView* logTable;            // New virtual table
    VirtualTableModel* virtualTableModel;  // Model for virtual table
    QTextEdit* hexView;
    QTextEdit* disassemblyView;
    QTextBrowser* detailsPane;

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
    LogClient* client;
    std::unique_ptr<CapstoneDisassembler> disassembler;
    SyntaxHighlighter* disassemblyHighlighter;
    SyntaxHighlighter* detailsHighlighter;
    SyntaxHighlightDelegate* tableDelegate;
    QTreeWidget* interruptsPanel;  // Left-side panel listing interrupts

    // Interrupt navigation state
    std::string currentSelectedInterrupt;  // raw interrupt number string
    int currentInterruptIndex;             // index within visible occurrences

    // Performance optimization members
    std::vector<QString> stringBuffers;               // Pre-allocated string buffers
    static constexpr size_t STRING_BUFFER_SIZE = 50;  // Number of string buffers
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
    void populateTable();            // Optimized version
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
    QString extractFileInfo(const std::string& func);                         // Extract file:line:column from function
    QString getSourceCodeSnippet(const QString& sourceFile, int sourceLine);  // Get source code snippet
    QColor getEntryTypeColor(EntryType type);                                 // Get color for entry type

    // Event handling
    bool eventFilter(QObject* obj, QEvent* event) override;
    void cancelSearch();                             // Cancel search and return to original position
    void onDetailsPaneLinkClicked(const QUrl& url);  // Handle clicks on VS Code links in details pane

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
    int findNextIretLine(int startLineNumber) const;  // Find the next iret instruction after a given line
};

#endif  // QEMU_LOG_VIEWER_H
