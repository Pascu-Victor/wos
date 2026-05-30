#include "coredump_browser.h"

#include <qcontainerfwd.h>
#include <qfont.h>
#include <qlogging.h>
#include <qmap.h>
#include <qnamespace.h>
#include <qoverload.h>
#include <qpoint.h>
#include <qprocess.h>
#include <qstringview.h>
#include <qtmetamacros.h>
#include <qtreewidget.h>
#include <qtypes.h>

#include <QAction>
#include <QDebug>
#include <QDir>
#include <QDirIterator>
#include <QFile>
#include <QFileInfo>
#include <QHeaderView>
#include <QMenu>
#include <QMessageBox>
#include <QVBoxLayout>
#include <algorithm>

#include "coredump_parser.h"

CoredumpBrowser::CoredumpBrowser(QWidget* parent) : QWidget(parent) { setup_ui(); }

CoredumpBrowser::~CoredumpBrowser() {
    if (extract_process && extract_process->state() != QProcess::NotRunning) {
        extract_process->kill();
        extract_process->waitForFinished(3000);
    }
}

void CoredumpBrowser::setup_ui() {
    auto* layout = new QVBoxLayout(this);
    layout->setContentsMargins(0, 0, 0, 0);

    tree = new QTreeWidget(this);
    tree->setHeaderLabels({"Name", "Interrupt", "Size", "Timestamp"});
    tree->setColumnCount(4);
    tree->header()->setStretchLastSection(true);
    tree->header()->setSectionResizeMode(0, QHeaderView::Stretch);
    tree->header()->setSectionResizeMode(1, QHeaderView::ResizeToContents);
    tree->header()->setSectionResizeMode(2, QHeaderView::ResizeToContents);
    tree->header()->setSectionResizeMode(3, QHeaderView::ResizeToContents);
    tree->setContextMenuPolicy(Qt::CustomContextMenu);
    tree->setAlternatingRowColors(true);
    tree->setRootIsDecorated(true);

    connect(tree, &QTreeWidget::itemActivated, this, &CoredumpBrowser::on_item_activated);
    connect(tree, &QTreeWidget::customContextMenuRequested, this, &CoredumpBrowser::on_context_menu);

    layout->addWidget(tree);
}

void CoredumpBrowser::set_directory(const QString& dir) {
    coredump_dir = dir;
    refresh();
}

void CoredumpBrowser::refresh() { scan_directory(); }

void CoredumpBrowser::scan_directory() {
    tree->clear();

    if (coredump_dir.isEmpty()) {
        return;
    }

    QDir root_dir(coredump_dir);
    if (!root_dir.exists()) {
        qDebug() << "Coredump directory does not exist:" << coredump_dir;
        return;
    }

    // Recursively find all *_coredump.bin files
    QMap<QString, QTreeWidgetItem*> group_items;  // subdirectory name -> tree item

    QDirIterator it(coredump_dir, {"*_coredump.bin"}, QDir::Files, QDirIterator::Subdirectories);
    int total_count = 0;

    while (it.hasNext()) {
        it.next();
        QFileInfo fi = it.fileInfo();
        QString full_path = fi.absoluteFilePath();

        // Determine group (parent directory relative to coredump root)
        QString rel_dir = root_dir.relativeFilePath(fi.absolutePath());
        if (rel_dir == ".") {
            rel_dir = "local";
        }

        // Get or create group item
        QTreeWidgetItem* group_item = group_items.value(rel_dir);
        if (!group_item) {
            group_item = new QTreeWidgetItem(tree);
            group_item->setText(0, rel_dir);
            group_item->setFlags(group_item->flags() & ~Qt::ItemIsSelectable);
            QFont f = group_item->font(0);
            f.setBold(true);
            group_item->setFont(0, f);
            group_items[rel_dir] = group_item;
        }

        // Parse binary name and quick-parse the coredump header for interrupt info
        QString binary_name = wosdbg::parse_binary_name_from_filename(fi.fileName());
        QString interrupt_str;
        QString timestamp_str;

        // Quick-read just the header to get interrupt number and timestamp
        QFile file(full_path);
        if (file.open(QIODevice::ReadOnly)) {
            QByteArray header_data = file.read(72 + 8);  // Read through int_num field
            if (header_data.size() >= 72) {
                auto dump = wosdbg::parse_core_dump(header_data.left(std::min(static_cast<qsizetype>(512), header_data.size())));
                // We can't fully parse with just the header, so read more
            }
            file.close();
        }

        // Try full parse for metadata (coredumps are usually small)
        QFile full_file(full_path);
        if (full_file.open(QIODevice::ReadOnly)) {
            QByteArray data = full_file.readAll();
            auto dump = wosdbg::parse_core_dump(data);
            if (dump) {
                interrupt_str = wosdbg::interrupt_name(dump->int_num);
                timestamp_str = QString::number(dump->timestamp);
            }
            full_file.close();
        }

        // Create file item
        auto* item = new QTreeWidgetItem(group_item);
        item->setText(0, binary_name.isEmpty() ? fi.fileName() : binary_name);
        item->setText(1, interrupt_str);
        item->setText(2, QString("%1 KB").arg(fi.size() / 1024));
        item->setText(3, timestamp_str);
        item->setData(0, Qt::UserRole, full_path);  // Store full path
        item->setToolTip(0, full_path);

        total_count++;
    }

    // Expand all groups
    tree->expandAll();

    // Update group labels with counts
    for (auto it = group_items.begin(); it != group_items.end(); ++it) {
        int count = it.value()->childCount();
        it.value()->setText(0, QString("%1 (%2)").arg(it.key()).arg(count));
    }

    qDebug() << "Found" << total_count << "coredump files in" << coredump_dir;
}

void CoredumpBrowser::on_item_activated(QTreeWidgetItem* item, int /*column*/) {
    QString path = item->data(0, Qt::UserRole).toString();
    if (!path.isEmpty()) {
        emit coredump_selected(path);
    }
}

void CoredumpBrowser::on_context_menu(const QPoint& pos) {
    QTreeWidgetItem* item = tree->itemAt(pos);
    if (!item) {
        return;
    }

    QString path = item->data(0, Qt::UserRole).toString();
    if (path.isEmpty()) {
        return;  // It's a group header
    }

    QMenu menu;
    QAction* open_action = menu.addAction("Open Coredump");
    menu.addSeparator();
    QAction* delete_action = menu.addAction("Delete");

    QAction* chosen = menu.exec(tree->viewport()->mapToGlobal(pos));
    if (chosen == open_action) {
        emit coredump_selected(path);
    } else if (chosen == delete_action) {
        auto reply = QMessageBox::question(this, "Delete Coredump", QString("Delete %1?").arg(QFileInfo(path).fileName()),
                                           QMessageBox::Yes | QMessageBox::No);
        if (reply == QMessageBox::Yes) {
            QFile::remove(path);
            refresh();
        }
    }
}

void CoredumpBrowser::extract_coredumps(bool cluster_mode) {
    if (extract_process && extract_process->state() != QProcess::NotRunning) {
        qWarning() << "Extraction already in progress";
        return;
    }

    // Find the extraction command relative to the repository root or CWD.
    QString script_path;
    QStringList search_paths = {
        QDir::currentPath() + "/bin/wos-extract-coredumps",
        QDir::currentPath() + "/../bin/wos-extract-coredumps",
        QDir::currentPath() + "/scripts/debug/extract_coredumps.sh",
        QDir::currentPath() + "/../scripts/debug/extract_coredumps.sh",
    };
    for (const auto& p : search_paths) {
        if (QFile::exists(p)) {
            script_path = p;
            break;
        }
    }

    if (script_path.isEmpty()) {
        emit extraction_finished(false, "wos-extract-coredumps not found");
        return;
    }

    extract_process = new QProcess(this);
    connect(extract_process, QOverload<int, QProcess::ExitStatus>::of(&QProcess::finished), this, &CoredumpBrowser::on_extraction_finished);

    QStringList args;
    if (cluster_mode) {
        args << "--cluster";
    }

    extract_process->setWorkingDirectory(QDir::currentPath());
    extract_process->start("bash", QStringList() << script_path << args);
}

void CoredumpBrowser::on_extraction_finished(int exit_code, QProcess::ExitStatus status) {
    QString output = extract_process->readAllStandardOutput();
    QString error_output = extract_process->readAllStandardError();

    bool success = (status == QProcess::NormalExit && exit_code == 0);
    QString message = success ? output : error_output;

    extract_process->deleteLater();
    extract_process = nullptr;

    // Auto-refresh after extraction
    refresh();

    emit extraction_finished(success, message);
}
