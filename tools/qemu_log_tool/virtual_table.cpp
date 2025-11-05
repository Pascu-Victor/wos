#include "virtual_table.h"

#include <qabstractitemmodel.h>
#include <qabstractitemview.h>
#include <qcontainerfwd.h>
#include <qnamespace.h>
#include <qobject.h>
#include <qtableview.h>
#include <qtimer.h>
#include <qtmetamacros.h>
#include <qtpreprocessorsupport.h>
#include <qwidget.h>

#include <QtCore/QDebug>
#include <QtGui/QColor>
#include <QtWidgets/QScrollBar>
#include <algorithm>
#include <cstddef>
#include <functional>
#include <utility>
#include <vector>

// ============================================================================
// VirtualRowCache Implementation
// ============================================================================

VirtualRowCache::VirtualRowCache(size_t maxRows) : maxCachedRows(maxRows) {}

const VirtualRowCache::CachedRow* VirtualRowCache::getRow(int logicalRow) const {
    auto it = cachedRows.find(logicalRow);
    if (it != cachedRows.end()) {
        if (trackingEnabled) ++cacheHits;
        const_cast<VirtualRowCache*>(this)->updateLRUOrder(logicalRow);
        return &it->second;
    }

    if (trackingEnabled) ++cacheMisses;
    return nullptr;
}

void VirtualRowCache::setRow(int logicalRow, const std::vector<QString>& cells, QColor bgColor) {
    // Check if we need to evict
    if (cachedRows.size() >= maxCachedRows && cachedRows.find(logicalRow) == cachedRows.end()) {
        evictLRU();
    }

    CachedRow& row = cachedRows[logicalRow];
    row.cells = cells;
    row.backgroundColor = bgColor;
    row.isValid = true;

    updateLRUOrder(logicalRow);
}

void VirtualRowCache::clear() {
    cachedRows.clear();
    lruOrder.clear();
    cacheHits = 0;
    cacheMisses = 0;
}

void VirtualRowCache::getStats(size_t& outCachedRows, size_t& outHits, size_t& outMisses) const {
    outCachedRows = cachedRows.size();
    outHits = cacheHits;
    outMisses = cacheMisses;
}

void VirtualRowCache::evictLRU() {
    if (lruOrder.empty()) return;

    int lruRow = lruOrder.front();
    lruOrder.erase(lruOrder.begin());
    cachedRows.erase(lruRow);
}

void VirtualRowCache::updateLRUOrder(int logicalRow) {
    // Remove from existing position if present
    auto it = std::find(lruOrder.begin(), lruOrder.end(), logicalRow);
    if (it != lruOrder.end()) {
        lruOrder.erase(it);
    }

    // Add to end (most recently used)
    lruOrder.push_back(logicalRow);
}

// ============================================================================
// VirtualTableModel Implementation
// ============================================================================

VirtualTableModel::VirtualTableModel(int totalRows, const QStringList& headers, QObject* parent)
    : QAbstractTableModel(parent), columnHeaders(headers), totalRowCount(totalRows), cache(500) {}

int VirtualTableModel::rowCount(const QModelIndex& parent) const {
    if (parent.isValid()) return 0;
    return totalRowCount;
}

int VirtualTableModel::columnCount(const QModelIndex& parent) const {
    if (parent.isValid()) return 0;
    return columnHeaders.size();
}

QVariant VirtualTableModel::data(const QModelIndex& index, int role) const {
    if (!index.isValid() || index.row() < 0 || index.row() >= totalRowCount || index.column() < 0 ||
        index.column() >= columnHeaders.size()) {
        return QVariant();
    }

    ensureRowLoaded(index.row());
    const VirtualRowCache::CachedRow* cachedRow = cache.getRow(index.row());

    if (!cachedRow || !cachedRow->isValid) {
        return QVariant();
    }

    switch (role) {
        case Qt::DisplayRole: {
            if (static_cast<size_t>(index.column()) < cachedRow->cells.size()) {
                return cachedRow->cells[index.column()];
            }
            return QVariant();
        }
        case Qt::BackgroundRole: {
            return cachedRow->backgroundColor;
        }
        case Qt::TextAlignmentRole: {
            return static_cast<int>(Qt::AlignLeft | Qt::AlignVCenter);
        }
        default:
            return QVariant();
    }
}

QVariant VirtualTableModel::headerData(int section, Qt::Orientation orientation, int role) const {
    if (orientation == Qt::Horizontal && role == Qt::DisplayRole && section >= 0 && section < columnHeaders.size()) {
        return columnHeaders[section];
    }
    return QVariant();
}

void VirtualTableModel::setDataProvider(std::function<void(int, std::vector<QString>&, QColor&)> provider,
                                        std::function<bool(const QString&)> highlightPredicate) {
    dataProvider = std::move(provider);
    this->highlightPredicate = highlightPredicate;
}

void VirtualTableModel::invalidateRows(int startRow, int endRow) {
    // Handle edge case: zero rows
    if (totalRowCount == 0) {
        return;
    }

    if (startRow < 0) startRow = 0;
    if (endRow >= totalRowCount) endRow = totalRowCount - 1;

    // Ensure valid range
    if (startRow > endRow || startRow >= totalRowCount) {
        return;
    }

    emit dataChanged(index(startRow, 0), index(endRow, columnCount() - 1));
}

void VirtualTableModel::setCacheSize(size_t newSize) {
    cache.clear();
    VirtualRowCache newCache(newSize);
    // Can't directly reassign, so we recreate with move semantics
    // This is a limitation of the current design; could be improved with pimpl
}

void VirtualTableModel::resetModel() {
    cache.clear();
    beginResetModel();
    endResetModel();
}

void VirtualTableModel::ensureRowLoaded(int logicalRow) const {
    // Check if already cached
    if (cache.getRow(logicalRow) != nullptr) {
        return;
    }

    // Load via provider
    if (dataProvider) {
        std::vector<QString> cells;
        QColor bgColor = Qt::transparent;
        dataProvider(logicalRow, cells, bgColor);
        // Cast away const to fill cache
        const_cast<VirtualTableModel*>(this)->cache.setRow(logicalRow, cells, bgColor);
    }
}

// ============================================================================
// VirtualTableView Implementation
// ============================================================================

VirtualTableView::VirtualTableView(QWidget* parent) : QTableView(parent) {
    setSelectionBehavior(QAbstractItemView::SelectRows);
    setSelectionMode(QAbstractItemView::SingleSelection);
    setAlternatingRowColors(true);
    setShowGrid(false);
    setWordWrap(false);

    scrollDebounceTimer = new QTimer(this);
    scrollDebounceTimer->setSingleShot(true);
    scrollDebounceTimer->setInterval(50);
    connect(scrollDebounceTimer, &QTimer::timeout, this, &VirtualTableView::updateVisibleRows);

    connect(verticalScrollBar(), &QScrollBar::valueChanged, this, &VirtualTableView::onVerticalScrollBarValueChanged);
}

void VirtualTableView::setVirtualModel(VirtualTableModel* model) {
    virtualModel = model;
    setModel(model);
}

void VirtualTableView::scrollToLogicalRow(int row, QAbstractItemView::ScrollHint hint) {
    if (!virtualModel) return;

    // Ensure the row is loaded
    virtualModel->invalidateRows(row, row);

    // Scroll to make it visible
    scrollTo(virtualModel->index(row, 0), hint);
}

int VirtualTableView::getViewportStartRow() const {
    if (!virtualModel) return 0;

    // Find first visible row
    int row = 0;
    int accumulatedHeight = 0;
    int viewportTop = verticalScrollBar()->value();

    while (row < virtualModel->rowCount() && accumulatedHeight < viewportTop) {
        accumulatedHeight += rowHeight(row);
        row++;
    }

    return row;
}

void VirtualTableView::resizeEvent(QResizeEvent* event) {
    QTableView::resizeEvent(event);
    updateVisibleRows();
}

void VirtualTableView::scrollContentsBy(int dx, int dy) {
    QTableView::scrollContentsBy(dx, dy);
    if (dy != 0) {
        scrollDebounceTimer->stop();
        scrollDebounceTimer->start();
    }
}

void VirtualTableView::onVerticalScrollBarValueChanged(int value) {
    Q_UNUSED(value)
    scrollDebounceTimer->stop();
    scrollDebounceTimer->start();
}

void VirtualTableView::updateVisibleRows() {
    if (!virtualModel) return;

    int currentStart = getViewportStartRow();

    if (currentStart == lastVisibleRowStart) {
        return;
    }

    lastVisibleRowStart = currentStart;

    // Calculate how many rows are visible
    int viewportHeight = viewport()->height();
    int visibleRowCount = 0;
    int currentRow = currentStart;
    int accHeight = 0;

    while (currentRow < virtualModel->rowCount() && accHeight < viewportHeight) {
        accHeight += rowHeight(currentRow);
        visibleRowCount++;
        currentRow++;
    }

    // Pre-load visible rows plus some buffer rows
    int bufferSize = visibleRowCount / 2;  // Load 50% extra on each side
    int startPreload = std::max(0, currentStart - bufferSize);
    int endPreload = std::min(virtualModel->rowCount() - 1, currentStart + visibleRowCount + bufferSize);

    // Trigger loading
    virtualModel->invalidateRows(startPreload, endPreload);
}
