#include "virtual_table.h"

#include <qabstractitemmodel.h>
#include <qabstractitemview.h>
#include <qcontainerfwd.h>
#include <qlogging.h>
#include <qnamespace.h>
#include <qobject.h>
#include <qtableview.h>
#include <qtimer.h>
#include <qtmetamacros.h>
#include <qtpreprocessorsupport.h>
#include <qwidget.h>

#include <QEvent>
#include <QPainter>
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

VirtualRowCache::VirtualRowCache(size_t max_rows) : maxCachedRows(max_rows) {}

const VirtualRowCache::CachedRow* VirtualRowCache::getRow(int logical_row) const {
    auto it = cachedRows.find(logical_row);
    if (it != cachedRows.end()) {
        if (trackingEnabled) {
            ++cacheHits;
        }
        const_cast<VirtualRowCache*>(this)->updateLRUOrder(logical_row);
        return &it->second;
    }

    if (trackingEnabled) {
        ++cacheMisses;
    }
    return nullptr;
}

void VirtualRowCache::setRow(int logical_row, const std::vector<QString>& cells, QColor bg_color) {
    // Check if we need to evict
    if (cachedRows.size() >= maxCachedRows && !cachedRows.contains(logical_row)) {
        evictLRU();
    }

    CachedRow& row = cachedRows[logical_row];
    row.cells = cells;
    row.backgroundColor = bg_color;
    row.isValid = true;

    updateLRUOrder(logical_row);
}

void VirtualRowCache::removeRow(int logical_row) {
    auto it = cachedRows.find(logical_row);
    if (it != cachedRows.end()) {
        cachedRows.erase(it);

        // Also remove from LRU order
        auto lru_it = std::ranges::find(lruOrder, logical_row);
        if (lru_it != lruOrder.end()) {
            lruOrder.erase(lru_it);
        }
    }
}

void VirtualRowCache::clear() {
    cachedRows.clear();
    lruOrder.clear();
    cacheHits = 0;
    cacheMisses = 0;
}

void VirtualRowCache::getStats(size_t& out_cached_rows, size_t& out_hits, size_t& out_misses) const {
    out_cached_rows = cachedRows.size();
    out_hits = cacheHits;
    out_misses = cacheMisses;
}

void VirtualRowCache::evictLRU() {
    if (lruOrder.empty()) {
        return;
    }

    int lru_row = lruOrder.front();
    lruOrder.erase(lruOrder.begin());
    cachedRows.erase(lru_row);
}

void VirtualRowCache::updateLRUOrder(int logical_row) {
    // Remove from existing position if present
    auto it = std::ranges::find(lruOrder, logical_row);
    if (it != lruOrder.end()) {
        lruOrder.erase(it);
    }

    // Add to end (most recently used)
    lruOrder.push_back(logical_row);
}

// ============================================================================
// VirtualTableModel Implementation
// ============================================================================

VirtualTableModel::VirtualTableModel(int total_rows, QStringList headers, QObject* parent)
    : QAbstractTableModel(parent), columnHeaders(std::move(headers)), totalRowCount(total_rows), cache(2048) {}

int VirtualTableModel::rowCount(const QModelIndex& parent) const {
    if (parent.isValid()) {
        return 0;
    }
    return totalRowCount;
}

int VirtualTableModel::columnCount(const QModelIndex& parent) const {
    if (parent.isValid()) {
        return 0;
    }
    return columnHeaders.size();
}

QVariant VirtualTableModel::data(const QModelIndex& index, int role) const {
    if (!index.isValid() || index.row() < 0 || index.row() >= totalRowCount || index.column() < 0 ||
        index.column() >= columnHeaders.size()) {
        return {};
    }

    ensureRowLoaded(index.row());
    const VirtualRowCache::CachedRow* cached_row = cache.getRow(index.row());

    if (!cached_row || !cached_row->isValid) {
        return {};
    }

    switch (role) {
        case Qt::DisplayRole: {
            if (static_cast<size_t>(index.column()) < cached_row->cells.size()) {
                return cached_row->cells[index.column()];
            }
            return {};
        }
        case Qt::BackgroundRole: {
            return cached_row->backgroundColor.alpha() == 0 ? QVariant{} : QVariant(cached_row->backgroundColor);
        }
        case Qt::TextAlignmentRole: {
            return static_cast<int>(Qt::AlignLeft | Qt::AlignVCenter);
        }
        default:
            return {};
    }
}

QVariant VirtualTableModel::headerData(int section, Qt::Orientation orientation, int role) const {
    if (orientation == Qt::Horizontal && role == Qt::DisplayRole && section >= 0 && section < columnHeaders.size()) {
        return columnHeaders[section];
    }
    return {};
}

void VirtualTableModel::setDataProvider(std::function<void(int, std::vector<QString>&, QColor&)> provider,
                                        std::function<bool(const QString&)> highlight_predicate) {
    dataProvider = std::move(provider);
    this->highlightPredicate = std::move(highlight_predicate);
}

void VirtualTableModel::invalidateRows(int start_row, int end_row) {
    // Handle edge case: zero rows
    if (totalRowCount == 0) {
        return;
    }

    start_row = std::max(start_row, 0);
    if (end_row >= totalRowCount) {
        end_row = totalRowCount - 1;
    }

    // Ensure valid range
    if (start_row > end_row || start_row >= totalRowCount) {
        return;
    }

    // Clear from cache so they are re-fetched
    for (int i = start_row; i <= end_row; ++i) {
        cache.removeRow(i);
    }

    emit dataChanged(index(start_row, 0), index(end_row, columnCount() - 1));
}

void VirtualTableModel::setCacheSize(size_t new_size) { cache = VirtualRowCache(std::max<size_t>(1, new_size)); }

void VirtualTableModel::prepareRows(int first, int last) const {
    for (int row = std::max(0, first); row <= std::min(last, totalRowCount - 1); ++row) {
        ensureRowLoaded(row);
    }
}

void VirtualTableModel::resetModel() {
    cache.clear();
    beginResetModel();
    endResetModel();
}

void VirtualTableModel::setRowCount(int row_count) {
    beginResetModel();
    totalRowCount = row_count;
    cache.clear();
    endResetModel();
}

void VirtualTableModel::ensureRowLoaded(int logical_row) const {
    // Check if already cached
    if (cache.getRow(logical_row) != nullptr) {
        return;
    }

    if (!loadingEnabled) {
        return;
    }

    // Load via provider
    if (dataProvider) {
        std::vector<QString> cells;
        QColor bg_color = Qt::transparent;
        dataProvider(logical_row, cells, bg_color);

        // A missing row must never become a cached, partially populated row.
        if (cells.size() == static_cast<size_t>(columnHeaders.size())) {
            const_cast<VirtualTableModel*>(this)->cache.setRow(logical_row, cells, bg_color);
        }
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

    setVerticalScrollMode(QAbstractItemView::ScrollPerItem);
    connect(verticalScrollBar(), &QScrollBar::valueChanged, this, &VirtualTableView::onVerticalScrollBarValueChanged);
}

void VirtualTableView::setVirtualModel(VirtualTableModel* model) {
    virtualModel = model;
    setModel(model);
    connect(model, &QAbstractItemModel::modelReset, this, [this]() {
        completeFrame = QPixmap();
        updateVisibleRows();
    });
}

void VirtualTableView::scrollToLogicalRow(int row, QAbstractItemView::ScrollHint hint) {
    if (!virtualModel) {
        return;
    }

    // Ensure the row is loaded
    if (fetchRows) {
        fetchRows(row, 128);
    }

    // Scroll to make it visible
    scrollTo(virtualModel->index(row, 0), hint);
}

int VirtualTableView::getViewportStartRow() const { return std::max(0, rowAt(0)); }

void VirtualTableView::resizeEvent(QResizeEvent* event) {
    QTableView::resizeEvent(event);
    updateVisibleRows();
}

void VirtualTableView::scrollContentsBy(int dx, int dy) {
    QTableView::scrollContentsBy(dx, dy);
    updateVisibleRows();
}

void VirtualTableView::onVerticalScrollBarValueChanged(int value) {
    Q_UNUSED(value)
    updateVisibleRows();
}

void VirtualTableView::updateVisibleRows() {
    if (!virtualModel) {
        return;
    }

    int current_start = getViewportStartRow();

    // Calculate how many rows are visible
    int viewport_height = viewport()->height();
    int visible_row_count = 0;
    int current_row = current_start;
    int acc_height = 0;

    while (current_row < virtualModel->rowCount() && acc_height < viewport_height) {
        acc_height += rowHeight(current_row);
        visible_row_count++;
        current_row++;
    }

    // Pre-load visible rows plus some buffer rows
    if (fetchRows) {
        fetchRows(current_start, visible_row_count);
    }
    int buffer_size = 128;
    int start_preload = std::max(0, current_start - buffer_size);
    int end_preload = std::min(virtualModel->rowCount() - 1, current_start + visible_row_count + buffer_size);

    // Trigger loading
    virtualModel->prepareRows(start_preload, end_preload);

    // Force a repaint of the viewport to ensure new data is drawn
    viewport()->update();
}

void VirtualTableView::setRangeLoader(std::function<void(int, int)> fetch, std::function<bool(int, int)> ready) {
    fetchRows = std::move(fetch);
    rowsReady = std::move(ready);
}

void VirtualTableView::paintEvent(QPaintEvent* event) {
    const int first = getViewportStartRow();
    const int last = rowAt(viewport()->height() - 1);
    const int count = virtualModel ? std::max(0, (last < 0 ? virtualModel->rowCount() - 1 : last) - first + 1) : 0;
    if (fetchRows && count) {
        fetchRows(first, count);
    }
    awaitingRows = rowsReady && count && !rowsReady(first, count);
    if (awaitingRows) {
        QPainter painter(viewport());
        painter.fillRect(viewport()->rect(), palette().base());
        if (!completeFrame.isNull()) {
            painter.drawPixmap(0, 0, completeFrame);
        }
        const QRect banner(12, 12, std::min(460, viewport()->width() - 24), 42);
        painter.fillRect(banner, palette().window());
        painter.setPen(palette().color(QPalette::Text));
        painter.drawRect(banner);
        painter.drawText(banner.adjusted(12, 0, -12, 0), Qt::AlignVCenter,
                         QString("Loading rows %1–%2 · previous view held").arg(first + 1).arg(first + count));
        return;
    }
    QTableView::paintEvent(event);
    if (!capturing && !captureQueued) {
        captureQueued = true;
        QTimer::singleShot(0, this, [this]() {
            captureQueued = false;
            if (awaitingRows || !isVisible()) {
                return;
            }
            capturing = true;
            completeFrame = viewport()->grab();
            capturing = false;
        });
    }
}

bool VirtualTableView::viewportEvent(QEvent* event) {
    // The retained frame is explicitly stale; it must not select rows at the new offset.
    if (awaitingRows && (event->type() == QEvent::MouseButtonPress || event->type() == QEvent::MouseButtonDblClick ||
                         event->type() == QEvent::ContextMenu)) {
        return true;
    }
    return QTableView::viewportEvent(event);
}
