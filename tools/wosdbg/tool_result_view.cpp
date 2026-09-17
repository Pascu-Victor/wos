#include "tool_result_view.h"

#include <QApplication>
#include <QClipboard>
#include <QFileDialog>
#include <QFontDatabase>
#include <QHBoxLayout>
#include <QHeaderView>
#include <QJsonArray>
#include <QJsonDocument>
#include <QLabel>
#include <QLineEdit>
#include <QPlainTextEdit>
#include <QPushButton>
#include <QSaveFile>
#include <QSplitter>
#include <QTabWidget>
#include <QTableWidget>
#include <QTreeWidget>
#include <QVBoxLayout>

#include "tool_presentation.h"

namespace {
QString preview(const QJsonValue& value) {
    if (value.isArray()) {
        return QString("%1 items — select to inspect rows").arg(value.toArray().size());
    }
    if (value.isObject()) {
        return QString("%1 fields").arg(value.toObject().size());
    }
    if (value.isNull()) {
        return "null";
    }
    const QString text = value.toVariant().toString();
    return text.size() > 300 ? text.left(300) + "…" : text;
}
}  // namespace

ToolResultView::ToolResultView(QWidget* parent) : QWidget(parent) {
    auto* layout = new QVBoxLayout(this);
    layout->setContentsMargins(0, 0, 0, 0);
    summary = new QLabel("Open an incident, log, or coredump to begin. Results will appear here.", this);
    summary->setWordWrap(true);
    summary->setTextFormat(Qt::PlainText);
    summary->setTextInteractionFlags(Qt::TextSelectableByMouse);
    layout->addWidget(summary);
    tabs = new QTabWidget(this);
    layout->addWidget(tabs, 1);
    auto* split = new QSplitter(Qt::Horizontal, tabs);
    tree = new QTreeWidget(split);
    tree->setObjectName("result_tree");
    tree->setHeaderLabels({"Evidence", "Value"});
    tree->setAlternatingRowColors(true);
    tree->header()->setSectionResizeMode(0, QHeaderView::ResizeToContents);
    tree->setMinimumWidth(180);
    split->addWidget(tree);
    auto* rows_widget = new QWidget(split);
    auto* rows_layout = new QVBoxLayout(rows_widget);
    filter = new QLineEdit(rows_widget);
    filter->setPlaceholderText("Filter this page…");
    filter->setClearButtonEnabled(true);
    rows_layout->addWidget(filter);
    table = new QTableWidget(rows_widget);
    table->setObjectName("result_table");
    table->setAlternatingRowColors(true);
    table->setEditTriggers(QAbstractItemView::NoEditTriggers);
    table->setSelectionBehavior(QAbstractItemView::SelectRows);
    table->setSelectionMode(QAbstractItemView::SingleSelection);
    table->setWordWrap(false);
    table->horizontalHeader()->setStretchLastSection(true);
    rows_layout->addWidget(table, 1);
    detail = new QPlainTextEdit(rows_widget);
    detail->setReadOnly(true);
    detail->setFont(QFontDatabase::systemFont(QFontDatabase::FixedFont));
    detail->hide();
    rows_layout->addWidget(detail, 1);
    table_status = new QLabel("Select an evidence list to view its rows.", rows_widget);
    table_status->setWordWrap(true);
    rows_layout->addWidget(table_status);
    auto* pages = new QHBoxLayout();
    previous_page = new QPushButton("Previous rows", rows_widget);
    previous_page->setObjectName("previous_result_page");
    next_page = new QPushButton("Next rows", rows_widget);
    next_page->setObjectName("next_result_page");
    previous_page->setEnabled(false);
    next_page->setEnabled(false);
    pages->addWidget(previous_page);
    pages->addWidget(next_page);
    pages->addStretch();
    rows_layout->addLayout(pages);
    connect(previous_page, &QPushButton::clicked, this, [this]() {
        page_start = qMax(0, page_start - 500);
        render_page();
    });
    connect(next_page, &QPushButton::clicked, this, [this]() {
        page_start += 500;
        render_page();
    });
    split->addWidget(rows_widget);
    split->setStretchFactor(1, 2);
    tabs->addTab(split, "Explore evidence");
    raw = new QPlainTextEdit(tabs);
    raw->setReadOnly(true);
    raw->setFont(QFontDatabase::systemFont(QFontDatabase::FixedFont));
    tabs->addTab(raw, "JSON / export");
    auto* buttons = new QHBoxLayout();
    auto* copy = new QPushButton("Copy JSON", this);
    auto* save = new QPushButton("Export result…", this);
    buttons->addStretch();
    buttons->addWidget(copy);
    buttons->addWidget(save);
    layout->addLayout(buttons);
    connect(copy, &QPushButton::clicked, this,
            [this]() { QApplication::clipboard()->setText(QString::fromUtf8(QJsonDocument(payload).toJson(QJsonDocument::Indented))); });
    connect(save, &QPushButton::clicked, this, [this]() {
        const QString path = QFileDialog::getSaveFileName(this, "Export complete result", "wosdbg-result.json", "JSON (*.json)");
        if (path.isEmpty()) {
            return;
        }
        QSaveFile file(path);
        const QByteArray data = QJsonDocument(payload).toJson(QJsonDocument::Indented);
        if (!file.open(QIODevice::WriteOnly) || file.write(data) != data.size() || !file.commit()) {
            table_status->setText("Export failed: " + file.errorString());
        } else {
            table_status->setText("Exported complete result to " + path);
        }
    });
    connect(tabs, &QTabWidget::currentChanged, this, [this](int index) {
        if (index == 1) {
            // Avoid serializing large payloads until explicitly requested.
            raw->setPlainText(QString::fromUtf8(QJsonDocument(payload).toJson(QJsonDocument::Indented)));
        }
    });
    connect(tree, &QTreeWidget::itemExpanded, this, [this](QTreeWidgetItem* item) {
        if (item->data(0, Qt::UserRole + 1).toBool()) {
            item->setData(0, Qt::UserRole + 1, false);
            qDeleteAll(item->takeChildren());
            populate(item, item->data(0, Qt::UserRole).value<QJsonValue>());
        }
    });
    connect(tree, &QTreeWidget::currentItemChanged, this, [this](QTreeWidgetItem* item) {
        if (!item) {
            return;
        }
        const auto value = item->data(0, Qt::UserRole).value<QJsonValue>();
        if (value.isArray()) {
            detail->hide();
            table->show();
            filter->show();
            table_collection = item->data(0, Qt::UserRole + 2).toString();
            show_table(value.toArray());
        } else if (value.isObject()) {
            QJsonObject context = value.toObject();
            if (item->parent()) {
                context["_collection"] = item->parent()->data(0, Qt::UserRole + 2).toString();
            }
            emit context_selected(context);
            item->setExpanded(true);
        } else if (!value.isUndefined()) {
            table->hide();
            filter->hide();
            detail->show();
            detail->setPlainText(value.isString() ? value.toString() : preview(value));
            table_status->setText(item->text(0));
        }
    });
    connect(table, &QTableWidget::itemSelectionChanged, this, [this]() {
        if (table->currentRow() < 0 || !table->item(table->currentRow(), 0)) {
            return;
        }
        const int index = table->item(table->currentRow(), 0)->data(Qt::UserRole).toInt();
        if (index < 0 || index >= table_rows.size()) {
            return;
        }
        QJsonObject context = table_rows[index].toObject();
        context["_collection"] = table_collection;
        emit context_selected(context);
    });
    connect(filter, &QLineEdit::textChanged, this, &ToolResultView::filter_rows);
    connect(table, &QTableWidget::itemDoubleClicked, this, [this](QTableWidgetItem* item) {
        const int index = item->data(Qt::UserRole).toInt();
        if (index < 0 || index >= table_rows.size()) {
            return;
        }
        delete inspected_row;
        inspected_row = new QTreeWidgetItem(tree, {QString("Selected row %1").arg(index + 1), "All fields"});
        inspected_row->setData(0, Qt::UserRole, QVariant::fromValue(table_rows[index]));
        populate(inspected_row, table_rows[index]);
        inspected_row->setExpanded(true);
        tree->scrollToItem(inspected_row);
    });
}

void ToolResultView::populate(QTreeWidgetItem* parent, const QJsonValue& value) {
    auto add = [parent](const QString& key, const QJsonValue& child) {
        auto* item = new QTreeWidgetItem(parent, {wosdbg::display_label(key), preview(child)});
        item->setData(0, Qt::UserRole, QVariant::fromValue(child));
        item->setData(0, Qt::UserRole + 2, key);
        item->setToolTip(1, child.isString() ? child.toString() : preview(child));
        if ((child.isArray() && !child.toArray().isEmpty()) || (child.isObject() && !child.toObject().isEmpty())) {
            item->setData(0, Qt::UserRole + 1, true);
            new QTreeWidgetItem(item, {"Expand to inspect"});
        }
    };
    if (value.isObject()) {
        const QJsonObject object = value.toObject();
        for (auto it = object.begin(); it != object.end(); ++it) {
            add(it.key(), it.value());
        }
    } else if (value.isArray()) {
        const QJsonArray array = value.toArray();
        const int count = qMin(500, static_cast<int>(array.size()));
        for (int index = 0; index < count; ++index) {
            add(QString::number(index + 1), array[index]);
        }
        if (array.size() > count) {
            new QTreeWidgetItem(parent, {"Preview limited", "Export JSON for all returned items"});
        }
    }
}

void ToolResultView::show_table(const QJsonArray& rows) {
    table_rows = rows;
    page_start = 0;
    render_page();
}

void ToolResultView::render_page() {
    detail->hide();
    table->show();
    filter->show();
    table->setSortingEnabled(false);
    table->blockSignals(true);
    table->clear();
    QStringList columns;
    const int count = qMin(500, static_cast<int>(table_rows.size()) - page_start);
    for (int index = page_start; index < page_start + count; ++index) {
        const QJsonObject object = table_rows[index].toObject();
        for (auto it = object.begin(); it != object.end(); ++it) {
            if (!columns.contains(it.key()) && columns.size() < 32) {
                columns.append(it.key());
            }
        }
    }
    if (columns.isEmpty()) {
        columns << "value";
    }
    table->setColumnCount(static_cast<int>(columns.size()));
    table->setRowCount(count);
    QStringList labels;
    for (const auto& key : columns) {
        labels << wosdbg::display_label(key);
    }
    table->setHorizontalHeaderLabels(labels);
    for (int row = 0; row < count; ++row) {
        for (int column = 0; column < columns.size(); ++column) {
            const QJsonValue entry = table_rows[page_start + row];
            const QJsonValue value = entry.isObject() ? entry.toObject()[columns[column]] : entry;
            auto* item = new QTableWidgetItem();
            item->setData(Qt::DisplayRole, value.isDouble() ? value.toVariant() : QVariant(preview(value)));
            item->setData(Qt::UserRole, page_start + row);
            item->setToolTip(value.isString() ? value.toString() : preview(value));
            table->setItem(row, column, item);
        }
    }
    table->setSortingEnabled(true);
    table->blockSignals(false);
    table->resizeColumnsToContents();
    for (int column = 0; column < columns.size(); ++column) {
        table->setColumnWidth(column, qMin(360, table->columnWidth(column)));
    }
    table_status->setText(QString("%1 · rows %2–%3 of %4 returned. Double-click a row to inspect all fields.")
                              .arg(wosdbg::display_label(table_collection))
                              .arg(count == 0 ? 0 : page_start + 1)
                              .arg(page_start + count)
                              .arg(table_rows.size()));
    previous_page->setEnabled(page_start > 0);
    next_page->setEnabled(page_start + count < table_rows.size());
    filter_rows();
}

void ToolResultView::filter_rows() {
    for (int row = 0; row < table->rowCount(); ++row) {
        bool matches = filter->text().isEmpty();
        for (int column = 0; column < table->columnCount() && !matches; ++column) {
            matches = table->item(row, column)->text().contains(filter->text(), Qt::CaseInsensitive);
        }
        table->setRowHidden(row, !matches);
    }
}

void ToolResultView::set_result(const QJsonObject& result, const QString& action_title) {
    payload = result;
    QString overview = wosdbg::result_summary(result).section('\n', 0, 5);
    overview.replace("Complete evidence and any omitted fields are in structuredContent.", "Explore the complete result below.");
    if (!action_title.isEmpty()) {
        overview.prepend(action_title + "\n");
    }
    if (result["degraded"].toBool()) {
        overview += "\nEvidence is degraded. Inspect issues before drawing conclusions.";
    }
    if (result["truncated"].toBool() || result["issuesTruncated"].toBool()) {
        overview += "\nResults are truncated. Narrow the scope or request another page.";
    }
    summary->setText(overview);
    detail->clear();
    detail->hide();
    table->show();
    filter->show();
    raw->clear();
    tabs->setCurrentIndex(0);
    tree->clear();
    inspected_row = nullptr;
    table->clear();
    table->setRowCount(0);
    table_rows = {};
    table_collection.clear();
    page_start = 0;
    previous_page->setEnabled(false);
    next_page->setEnabled(false);
    filter->clear();
    populate(tree->invisibleRootItem(), result);
    table_status->setText("Select an evidence list to view its rows.");
    for (int index = 0; index < tree->topLevelItemCount(); ++index) {
        auto* item = tree->topLevelItem(index);
        const auto value = item->data(0, Qt::UserRole).value<QJsonValue>();
        if (value.isArray() && !value.toArray().isEmpty()) {
            tree->setCurrentItem(item);
            break;
        }
    }
}
