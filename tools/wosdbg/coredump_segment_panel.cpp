#include "coredump_segment_panel.h"

#include <qabstractitemview.h>
#include <qcolor.h>
#include <qdockwidget.h>
#include <qnamespace.h>
#include <qobject.h>
#include <qtablewidget.h>
#include <qtmetamacros.h>
#include <qwidget.h>

#include <QFont>
#include <QHeaderView>
#include <QVBoxLayout>
#include <algorithm>
#include <cstddef>
#include <cstdint>

#include "coredump_parser.h"

CoredumpSegmentPanel::CoredumpSegmentPanel(QWidget* parent) : QDockWidget("Segments", parent) { setup_ui(); }

void CoredumpSegmentPanel::setup_ui() {
    auto* container = new QWidget(this);
    auto* layout = new QVBoxLayout(container);
    layout->setContentsMargins(4, 4, 4, 4);

    table = new QTableWidget(container);
    table->setColumnCount(6);
    table->setHorizontalHeaderLabels({"#", "Type", "VA Start", "VA End", "Size", "Present"});
    table->verticalHeader()->setVisible(false);
    table->horizontalHeader()->setStretchLastSection(true);
    table->setAlternatingRowColors(true);
    table->setSelectionBehavior(QAbstractItemView::SelectRows);
    table->setSelectionMode(QAbstractItemView::SingleSelection);

    connect(table, &QTableWidget::cellDoubleClicked, this, &CoredumpSegmentPanel::on_segment_activated);

    layout->addWidget(table);
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

void CoredumpSegmentPanel::load_core_dump(const wosdbg::CoreDump& dump) {
    table->clearContents();
    seg_infos.clear();

    int count = static_cast<int>(std::min(dump.segment_count, static_cast<uint64_t>(dump.segments.size())));
    table->setRowCount(count);

    for (int i = 0; i < count; ++i) {
        const auto& seg = dump.segments[static_cast<size_t>(i)];

        table->setItem(i, 0, make_mono_item(QString::number(i)));
        table->setItem(i, 1, make_mono_item(seg.type_name()));
        table->setItem(i, 2, make_mono_item(wosdbg::format_u64(seg.vaddr)));
        table->setItem(i, 3, make_mono_item(wosdbg::format_u64(seg.vaddr_end())));
        table->setItem(i, 4, make_mono_item(QString("0x%1").arg(seg.size, 0, 16)));

        auto* present_item = make_mono_item(seg.is_present() ? "Yes" : "No");
        if (!seg.is_present()) {
            present_item->setForeground(QColor(255, 100, 100));
        }
        table->setItem(i, 5, present_item);

        seg_infos.push_back({.index = i, .vaStart = seg.vaddr, .vaEnd = seg.vaddr_end(), .present = seg.is_present()});
    }

    table->resizeColumnsToContents();
}

void CoredumpSegmentPanel::clear() {
    table->setRowCount(0);
    seg_infos.clear();
}

void CoredumpSegmentPanel::on_segment_activated(int row, int /*column*/) {
    if (row < 0 || static_cast<size_t>(row) >= seg_infos.size()) {
        return;
    }
    const auto& info = seg_infos[static_cast<size_t>(row)];
    if (info.present) {
        emit dump_segment_requested(info.index, info.vaStart, info.vaEnd);
    }
}
