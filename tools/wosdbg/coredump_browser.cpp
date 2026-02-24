#include "coredump_browser.h"

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

#include "coredump_parser.h"

CoredumpBrowser::CoredumpBrowser(QWidget* parent) : QWidget(parent) { setupUI(); }

CoredumpBrowser::~CoredumpBrowser() {
    if (extractProcess_ && extractProcess_->state() != QProcess::NotRunning) {
        extractProcess_->kill();
        extractProcess_->waitForFinished(3000);
    }
}

void CoredumpBrowser::setupUI() {
    auto* layout = new QVBoxLayout(this);
    layout->setContentsMargins(0, 0, 0, 0);

    tree_ = new QTreeWidget(this);
    tree_->setHeaderLabels({"Name", "Interrupt", "Size", "Timestamp"});
    tree_->setColumnCount(4);
    tree_->header()->setStretchLastSection(true);
    tree_->header()->setSectionResizeMode(0, QHeaderView::Stretch);
    tree_->header()->setSectionResizeMode(1, QHeaderView::ResizeToContents);
    tree_->header()->setSectionResizeMode(2, QHeaderView::ResizeToContents);
    tree_->header()->setSectionResizeMode(3, QHeaderView::ResizeToContents);
    tree_->setContextMenuPolicy(Qt::CustomContextMenu);
    tree_->setAlternatingRowColors(true);
    tree_->setRootIsDecorated(true);

    connect(tree_, &QTreeWidget::itemActivated, this, &CoredumpBrowser::onItemActivated);
    connect(tree_, &QTreeWidget::customContextMenuRequested, this, &CoredumpBrowser::onContextMenu);

    layout->addWidget(tree_);
}

void CoredumpBrowser::setDirectory(const QString& dir) {
    coredumpDir_ = dir;
    refresh();
}

void CoredumpBrowser::refresh() { scanDirectory(); }

void CoredumpBrowser::scanDirectory() {
    tree_->clear();

    if (coredumpDir_.isEmpty()) {
        return;
    }

    QDir root_dir(coredumpDir_);
    if (!root_dir.exists()) {
        qDebug() << "Coredump directory does not exist:" << coredumpDir_;
        return;
    }

    // Recursively find all *_coredump.bin files
    QMap<QString, QTreeWidgetItem*> group_items;  // subdirectory name -> tree item

    QDirIterator it(coredumpDir_, {"*_coredump.bin"}, QDir::Files, QDirIterator::Subdirectories);
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
            group_item = new QTreeWidgetItem(tree_);
            group_item->setText(0, rel_dir);
            group_item->setFlags(group_item->flags() & ~Qt::ItemIsSelectable);
            QFont f = group_item->font(0);
            f.setBold(true);
            group_item->setFont(0, f);
            group_items[rel_dir] = group_item;
        }

        // Parse binary name and quick-parse the coredump header for interrupt info
        QString binary_name = wosdbg::parseBinaryNameFromFilename(fi.fileName());
        QString interrupt_str;
        QString timestamp_str;

        // Quick-read just the header to get interrupt number and timestamp
        QFile file(full_path);
        if (file.open(QIODevice::ReadOnly)) {
            QByteArray header_data = file.read(72 + 8);  // Read through intNum field
            if (header_data.size() >= 72) {
                auto dump = wosdbg::parseCoreDump(header_data.left(std::min(static_cast<qsizetype>(512), header_data.size())));
                // We can't fully parse with just the header, so read more
            }
            file.close();
        }

        // Try full parse for metadata (coredumps are usually small)
        QFile full_file(full_path);
        if (full_file.open(QIODevice::ReadOnly)) {
            QByteArray data = full_file.readAll();
            auto dump = wosdbg::parseCoreDump(data);
            if (dump) {
                interrupt_str = wosdbg::interruptName(dump->intNum);
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
    tree_->expandAll();

    // Update group labels with counts
    for (auto it = group_items.begin(); it != group_items.end(); ++it) {
        int count = it.value()->childCount();
        it.value()->setText(0, QString("%1 (%2)").arg(it.key()).arg(count));
    }

    qDebug() << "Found" << total_count << "coredump files in" << coredumpDir_;
}

void CoredumpBrowser::onItemActivated(QTreeWidgetItem* item, int /*column*/) {
    QString path = item->data(0, Qt::UserRole).toString();
    if (!path.isEmpty()) {
        emit coredumpSelected(path);
    }
}

void CoredumpBrowser::onContextMenu(const QPoint& pos) {
    QTreeWidgetItem* item = tree_->itemAt(pos);
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

    QAction* chosen = menu.exec(tree_->viewport()->mapToGlobal(pos));
    if (chosen == open_action) {
        emit coredumpSelected(path);
    } else if (chosen == delete_action) {
        auto reply = QMessageBox::question(this, "Delete Coredump", QString("Delete %1?").arg(QFileInfo(path).fileName()),
                                           QMessageBox::Yes | QMessageBox::No);
        if (reply == QMessageBox::Yes) {
            QFile::remove(path);
            refresh();
        }
    }
}

void CoredumpBrowser::extractCoredumps(bool cluster_mode) {
    if (extractProcess_ && extractProcess_->state() != QProcess::NotRunning) {
        qWarning() << "Extraction already in progress";
        return;
    }

    // Find the script relative to the binary location or CWD
    QString script_path;
    QStringList search_paths = {
        QDir::currentPath() + "/scripts/extract_coredumps.sh",
        QDir::currentPath() + "/../scripts/extract_coredumps.sh",
    };
    for (const auto& p : search_paths) {
        if (QFile::exists(p)) {
            script_path = p;
            break;
        }
    }

    if (script_path.isEmpty()) {
        emit extractionFinished(false, "extract_coredumps.sh not found");
        return;
    }

    extractProcess_ = new QProcess(this);
    connect(extractProcess_, QOverload<int, QProcess::ExitStatus>::of(&QProcess::finished), this, &CoredumpBrowser::onExtractionFinished);

    QStringList args;
    if (cluster_mode) {
        args << "--cluster";
    }

    extractProcess_->setWorkingDirectory(QDir::currentPath());
    extractProcess_->start("bash", QStringList() << script_path << args);
}

void CoredumpBrowser::onExtractionFinished(int exit_code, QProcess::ExitStatus status) {
    QString output = extractProcess_->readAllStandardOutput();
    QString error_output = extractProcess_->readAllStandardError();

    bool success = (status == QProcess::NormalExit && exit_code == 0);
    QString message = success ? output : error_output;

    extractProcess_->deleteLater();
    extractProcess_ = nullptr;

    // Auto-refresh after extraction
    refresh();

    emit extractionFinished(success, message);
}
