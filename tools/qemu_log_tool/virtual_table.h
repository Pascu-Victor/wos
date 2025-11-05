#ifndef VIRTUAL_TABLE_H
#define VIRTUAL_TABLE_H

#include <QtCore/QAbstractTableModel>
#include <QtCore/QTimer>
#include <QtWidgets/QTableView>
#include <cstddef>
#include <memory>
#include <unordered_map>
#include <vector>

// Forward declaration
struct LogEntry;

/**
 * Virtual row cache that maintains a fixed number of rows in memory using LRU eviction
 */
class VirtualRowCache {
   public:
    struct CachedRow {
        std::vector<QString> cells;  // Column data
        QColor backgroundColor;
        bool isValid;
    };

    /**
     * Create cache with specified size
     * @param maxRows Maximum rows to keep in cache
     */
    explicit VirtualRowCache(size_t maxRows = 500);

    /**
     * Get a cached row or nullptr if not in cache
     */
    const CachedRow* getRow(int logicalRow) const;

    /**
     * Add/update a row in cache
     */
    void setRow(int logicalRow, const std::vector<QString>& cells, QColor bgColor);

    /**
     * Clear all cached rows
     */
    void clear();

    /**
     * Get cache statistics
     */
    void getStats(size_t& outCachedRows, size_t& outHits, size_t& outMisses) const;

    /**
     * Enable/disable statistics tracking
     */
    void setTrackingEnabled(bool enabled) { trackingEnabled = enabled; }

   private:
    size_t maxCachedRows;
    std::unordered_map<int, CachedRow> cachedRows;
    std::vector<int> lruOrder;  // Tracks access order for LRU eviction

    // Statistics
    mutable size_t cacheHits = 0;
    mutable size_t cacheMisses = 0;
    bool trackingEnabled = true;

    void evictLRU();
    void updateLRUOrder(int logicalRow);
};

/**
 * Virtual table model that manages lazy-loading of rows
 */
class VirtualTableModel : public QAbstractTableModel {
    Q_OBJECT

   public:
    /**
     * Initialize model with total row count and column headers
     */
    VirtualTableModel(int totalRows, const QStringList& headers, QObject* parent = nullptr);

    int rowCount(const QModelIndex& parent = QModelIndex()) const override;
    int columnCount(const QModelIndex& parent = QModelIndex()) const override;
    QVariant data(const QModelIndex& index, int role = Qt::DisplayRole) const override;
    QVariant headerData(int section, Qt::Orientation orientation, int role = Qt::DisplayRole) const override;

    /**
     * Set the data provider callback
     * This is called when a row needs to be loaded from disk/memory
     * Callback signature: void(int logicalRow, std::vector<QString>& outCells, QColor& outBgColor)
     */
    void setDataProvider(std::function<void(int, std::vector<QString>&, QColor&)> provider,
                         std::function<bool(const QString&)> highlightPredicate = nullptr);

    /**
     * Mark a range of rows as potentially changed (triggers refresh)
     */
    void invalidateRows(int startRow, int endRow);

    /**
     * Get access to cache for statistics
     */
    const VirtualRowCache& getCache() const { return cache; }

    /**
     * Set cache size
     */
    void setCacheSize(size_t newSize);

    /**
     * Manually reset the model (clear all data)
     */
    void resetModel();

    /**
     * Set the total row count (must be called before resetting)
     */
    void setRowCount(int rowCount) { totalRowCount = rowCount; }

   private:
    QStringList columnHeaders;
    int totalRowCount;
    VirtualRowCache cache;
    std::function<void(int, std::vector<QString>&, QColor&)> dataProvider;
    std::function<bool(const QString&)> highlightPredicate;

    void ensureRowLoaded(int logicalRow) const;
};

/**
 * Virtual table view with scrollbar synchronization
 */
class VirtualTableView : public QTableView {
    Q_OBJECT

   public:
    explicit VirtualTableView(QWidget* parent = nullptr);

    /**
     * Set the virtual model
     */
    void setVirtualModel(VirtualTableModel* model);

    /**
     * Scroll to a specific logical row
     */
    void scrollToLogicalRow(int row, QAbstractItemView::ScrollHint hint = QAbstractItemView::EnsureVisible);

    /**
     * Get current viewport scroll position
     */
    int getViewportStartRow() const;

   protected:
    void resizeEvent(QResizeEvent* event) override;
    void scrollContentsBy(int dx, int dy) override;

   private slots:
    void onVerticalScrollBarValueChanged(int value);

   private:
    VirtualTableModel* virtualModel = nullptr;
    QTimer* scrollDebounceTimer = nullptr;
    int lastVisibleRowStart = -1;

    void updateVisibleRows();
};

#endif  // VIRTUAL_TABLE_H
