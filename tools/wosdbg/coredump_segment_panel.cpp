#include "coredump_segment_panel.h"

#include <QFont>
#include <QHeaderView>
#include <QVBoxLayout>

#include "coredump_parser.h"

CoredumpSegmentPanel::CoredumpSegmentPanel(QWidget* parent) : QDockWidget("Segments", parent) { setupUI(); }

void CoredumpSegmentPanel::setupUI() {
    auto* container = new QWidget(this);
    auto* layout = new QVBoxLayout(container);
    layout->setContentsMargins(4, 4, 4, 4);

    table_ = new QTableWidget(container);
    table_->setColumnCount(6);
    table_->setHorizontalHeaderLabels({"#", "Type", "VA Start", "VA End", "Size", "Present"});
    table_->verticalHeader()->setVisible(false);
    table_->horizontalHeader()->setStretchLastSection(true);
    table_->setAlternatingRowColors(true);
    table_->setSelectionBehavior(QAbstractItemView::SelectRows);
    table_->setSelectionMode(QAbstractItemView::SingleSelection);

    connect(table_, &QTableWidget::cellDoubleClicked, this, &CoredumpSegmentPanel::onSegmentActivated);

    layout->addWidget(table_);
    setWidget(container);
}

static QTableWidgetItem* make_mono_item(const QString& text) {
    auto* item = new QTableWidgetItem(text);
    item->setFlags(item->flags() & ~Qt::ItemIsEditable);
    QFont f("Monospace", 9);
    f.setStyleHint(QFont::Monospace);
    item->setFont(f);
    return item;
}

void CoredumpSegmentPanel::loadCoreDump(const wosdbg::CoreDump& dump) {
    table_->clearContents();
    segInfos_.clear();

    int count = static_cast<int>(std::min(dump.segmentCount, static_cast<uint64_t>(dump.segments.size())));
    table_->setRowCount(count);

    for (int i = 0; i < count; ++i) {
        const auto& seg = dump.segments[static_cast<size_t>(i)];

        table_->setItem(i, 0, make_mono_item(QString::number(i)));
        table_->setItem(i, 1, make_mono_item(seg.typeName()));
        table_->setItem(i, 2, make_mono_item(wosdbg::formatU64(seg.vaddr)));
        table_->setItem(i, 3, make_mono_item(wosdbg::formatU64(seg.vaddrEnd())));
        table_->setItem(i, 4, make_mono_item(QString("0x%1").arg(seg.size, 0, 16)));

        auto* present_item = make_mono_item(seg.isPresent() ? "Yes" : "No");
        if (!seg.isPresent()) {
            present_item->setForeground(QColor(255, 100, 100));
        }
        table_->setItem(i, 5, present_item);

        segInfos_.push_back({i, seg.vaddr, seg.vaddrEnd(), seg.isPresent()});
    }

    table_->resizeColumnsToContents();
}

void CoredumpSegmentPanel::clear() {
    table_->setRowCount(0);
    segInfos_.clear();
}

void CoredumpSegmentPanel::onSegmentActivated(int row, int /*column*/) {
    if (row < 0 || static_cast<size_t>(row) >= segInfos_.size()) {
        return;
    }
    const auto& info = segInfos_[static_cast<size_t>(row)];
    if (info.present) {
        emit dumpSegmentRequested(info.index, info.vaStart, info.vaEnd);
    }
}
