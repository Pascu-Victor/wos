#include "tool_arguments_form.h"

#include <QCheckBox>
#include <QComboBox>
#include <QFile>
#include <QFileDialog>
#include <QFormLayout>
#include <QHBoxLayout>
#include <QJsonArray>
#include <QJsonDocument>
#include <QJsonParseError>
#include <QLabel>
#include <QLineEdit>
#include <QPushButton>
#include <QRegularExpression>
#include <QRegularExpressionValidator>
#include <QVBoxLayout>
#include <functional>
#include <limits>

#include "tool_presentation.h"

class ToolArgumentField final : public QWidget {
   public:
    ToolArgumentField(const QString& name, const QJsonObject& schema, const QJsonValue& suggestion, const std::function<void()>& changed,
                      QWidget* parent)
        : QWidget(parent), schema(schema), type(schema["type"].toString("string")), changed(changed) {
        setObjectName("argument_" + name);
        setToolTip(schema["description"].toString());
        auto* layout = new QVBoxLayout(this);
        layout->setContentsMargins(0, 0, 0, 0);
        if (schema.contains("x-wosdbg-confirmation")) {
            checkbox = new QCheckBox(schema["x-wosdbg-confirmation"].toString(), this);
            checkbox->setObjectName("confirm_" + name);
            // A confirmation is never remembered or preselected.
            connect(checkbox, &QCheckBox::toggled, this, changed);
            layout->addWidget(checkbox);
        } else if (type == "boolean") {
            checkbox = new QCheckBox("Enabled", this);
            checkbox->setChecked(suggestion.toBool());
            connect(checkbox, &QCheckBox::toggled, this, changed);
            layout->addWidget(checkbox);
        } else if (schema.contains("enum")) {
            combo = new QComboBox(this);
            for (const auto& value : schema["enum"].toArray()) {
                combo->addItem(wosdbg::display_label(value.toString()), value.toVariant());
            }
            const int index = combo->findData(suggestion.toVariant());
            if (index >= 0) {
                combo->setCurrentIndex(index);
            }
            connect(combo, &QComboBox::currentIndexChanged, this, changed);
            layout->addWidget(combo);
        } else if (type == "array") {
            rows_layout = new QVBoxLayout();
            layout->addLayout(rows_layout);
            auto* add = new QPushButton("Add item", this);
            add->setObjectName("add_" + name);
            layout->addWidget(add, 0, Qt::AlignLeft);
            connect(add, &QPushButton::clicked, this, [this]() { add_row({}); });
            for (const auto& value : suggestion.toArray()) {
                add_row(value);
            }
        } else if (type == "object" && schema.contains("properties")) {
            nested = new ToolArgumentsForm(this);
            nested->set_schema(schema, suggestion.toObject());
            connect(nested, &ToolArgumentsForm::changed, this, changed);
            layout->addWidget(nested);
        } else if (type == "object") {
            imported = suggestion;
            import_status = new QLabel(suggestion.isObject() ? "Object supplied by previous result" : "No document selected", this);
            import_status->setWordWrap(true);
            import_status->setTextFormat(Qt::PlainText);
            layout->addWidget(import_status);
            auto* browse = new QPushButton("Import document…", this);
            layout->addWidget(browse, 0, Qt::AlignLeft);
            connect(browse, &QPushButton::clicked, this, [this, name]() {
                const QString path = QFileDialog::getOpenFileName(this, "Import " + wosdbg::display_label(name), {},
                                                                  "JSON documents (*.json);;All files (*)");
                if (path.isEmpty()) {
                    return;
                }
                QFile file(path);
                if (!file.open(QIODevice::ReadOnly) || file.size() > 1024 * 1024) {
                    imported = {};
                    import_status->setText("Cannot read document (maximum 1 MiB).");
                    this->changed();
                    return;
                }
                QJsonParseError error;
                const QJsonDocument document = QJsonDocument::fromJson(file.readAll(), &error);
                if (error.error != QJsonParseError::NoError || !document.isObject()) {
                    imported = {};
                    import_status->setText("Select a valid exported JSON object.");
                } else {
                    const QJsonObject object = document.object();
                    imported = object[name].isObject() ? object[name] : QJsonValue(object);
                    import_status->setText(path);
                }
                this->changed();
            });
        } else {
            auto* row = new QHBoxLayout();
            edit = new QLineEdit(this);
            edit->setObjectName("input_" + name);
            edit->setAccessibleName(wosdbg::display_label(name));
            edit->setText(suggestion.isString() ? suggestion.toString() : suggestion.toVariant().toString());
            if (name == "leaseToken") {
                edit->setEchoMode(QLineEdit::Password);
            }
            if (type == "integer") {
                edit->setValidator(new QRegularExpressionValidator(QRegularExpression("-?[0-9]+"), edit));
                edit->setPlaceholderText("Whole number");
            } else if (name == "address") {
                edit->setPlaceholderText("0x… or decimal address");
            } else {
                edit->setPlaceholderText(schema["description"].toString());
            }
            connect(edit, &QLineEdit::textChanged, this, changed);
            row->addWidget(edit);
            if (schema.contains("x-wosdbg-path")) {
                auto* browse = new QPushButton("Browse…", this);
                browse->setToolTip("Choose a local path. In remote mode, enter a path on the backend host.");
                row->addWidget(browse);
                connect(browse, &QPushButton::clicked, this, [this]() {
                    const QString path = this->schema["x-wosdbg-path"].toString() == "save"
                                             ? QFileDialog::getSaveFileName(this, "Incident destination", edit->text())
                                             : QFileDialog::getOpenFileName(this, "Select file", edit->text());
                    if (!path.isEmpty()) {
                        edit->setText(path);
                    }
                });
                if (name == "path") {
                    auto* directory = new QPushButton("Folder…", this);
                    row->addWidget(directory);
                    connect(directory, &QPushButton::clicked, this, [this]() {
                        const QString path = QFileDialog::getExistingDirectory(this, "Select incident directory", edit->text());
                        if (!path.isEmpty()) {
                            edit->setText(path);
                        }
                    });
                }
            }
            layout->addLayout(row);
        }
    }

    QJsonValue value(QString* error) const {
        if (schema.contains("x-wosdbg-confirmation")) {
            if (!checkbox->isChecked()) {
                *error = "Confirm this operation before running it.";
                return {};
            }
            return schema["enum"].toArray().first();
        }
        if (checkbox) {
            return checkbox->isChecked();
        }
        if (combo) {
            return QJsonValue::fromVariant(combo->currentData());
        }
        if (type == "array") {
            QJsonArray result;
            for (auto* row : rows) {
                result.append(row->value(error));
                if (!error->isEmpty()) {
                    return {};
                }
            }
            return result;
        }
        if (nested) {
            return nested->arguments(error);
        }
        if (type == "object") {
            if (!imported.isObject()) {
                *error = "Import an exported document first.";
            }
            return imported;
        }
        const QString text = edit->text();
        if (type == "integer") {
            bool ok = false;
            const qlonglong number = text.toLongLong(&ok);
            if (!ok || (schema.contains("minimum") && number < schema["minimum"].toInteger()) ||
                (schema.contains("maximum") && number > schema["maximum"].toInteger())) {
                *error = "Enter a whole number within the allowed range.";
                return {};
            }
            return QJsonValue(number);
        }
        if (type == "number") {
            bool ok = false;
            const double number = text.toDouble(&ok);
            if (!ok) {
                *error = "Enter a number.";
            }
            return number;
        }
        if (text.trimmed().isEmpty()) {
            *error = "Enter a value, or turn off this optional field.";
        }
        return text;
    }

   private:
    void add_row(const QJsonValue& suggestion) {
        if (rows.size() >= schema["maxItems"].toInt(128)) {
            return;
        }
        auto* container = new QWidget(this);
        auto* layout = new QHBoxLayout(container);
        layout->setContentsMargins(0, 0, 0, 0);
        auto* field = new ToolArgumentField("item", schema["items"].toObject(), suggestion, changed, container);
        auto* remove = new QPushButton("Remove", container);
        layout->addWidget(field, 1);
        layout->addWidget(remove, 0, Qt::AlignTop);
        rows.append(field);
        rows_layout->addWidget(container);
        connect(remove, &QPushButton::clicked, this, [this, container, field]() {
            rows.removeOne(field);
            rows_layout->removeWidget(container);
            container->deleteLater();
            changed();
        });
        changed();
    }

    QJsonObject schema;
    QString type;
    std::function<void()> changed;
    QCheckBox* checkbox = nullptr;
    QComboBox* combo = nullptr;
    QLineEdit* edit = nullptr;
    ToolArgumentsForm* nested = nullptr;
    QVBoxLayout* rows_layout = nullptr;
    QList<ToolArgumentField*> rows;
    QJsonValue imported;
    QLabel* import_status = nullptr;
};

ToolArgumentsForm::ToolArgumentsForm(QWidget* parent) : QWidget(parent), form(new QFormLayout(this)) {
    form->setFieldGrowthPolicy(QFormLayout::ExpandingFieldsGrow);
    form->setRowWrapPolicy(QFormLayout::WrapLongRows);
    form->setVerticalSpacing(12);
}

void ToolArgumentsForm::set_schema(const QJsonObject& schema, const QJsonObject& suggestions) {
    fields.clear();
    while (form->rowCount() > 0) {
        form->removeRow(0);
    }
    const QJsonObject properties = schema["properties"].toObject();
    const QJsonArray required = schema["required"].toArray();
    // Required inputs first; all optional inputs retain an explicit omission switch.
    for (bool required_pass : {true, false}) {
        for (auto it = properties.begin(); it != properties.end(); ++it) {
            const bool is_required = required.contains(it.key());
            if (is_required != required_pass) {
                continue;
            }
            const QJsonObject definition = it.value().toObject();
            auto* editor = new ToolArgumentField(it.key(), definition, suggestions.value(it.key()), [this]() { emit changed(); }, this);
            const QString title = definition["title"].toString(wosdbg::display_label(it.key()));
            QCheckBox* enabled = nullptr;
            if (is_required) {
                auto* label = new QLabel(title + " *", this);
                label->setBuddy(editor->findChild<QLineEdit*>());
                label->setToolTip(definition["description"].toString());
                form->addRow(label, editor);
            } else {
                enabled = new QCheckBox(title, this);
                enabled->setObjectName("include_" + it.key());
                enabled->setToolTip("Include this option. Leave unchecked to use the backend default.\n" +
                                    definition["description"].toString());
                enabled->setChecked(suggestions.contains(it.key()) && !definition.contains("x-wosdbg-confirmation"));
                editor->setEnabled(enabled->isChecked());
                connect(enabled, &QCheckBox::toggled, editor, &QWidget::setEnabled);
                connect(enabled, &QCheckBox::toggled, this, &ToolArgumentsForm::changed);
                form->addRow(enabled, editor);
            }
            fields.insert(it.key(), {editor, enabled});
        }
    }
    if (properties.isEmpty()) {
        form->addRow(new QLabel("Ready to run. This action needs no inputs.", this));
    }
}

QJsonObject ToolArgumentsForm::arguments(QString* error) const {
    QString field_error;
    QJsonObject result;
    for (auto it = fields.begin(); it != fields.end(); ++it) {
        if (it->enabled && !it->enabled->isChecked()) {
            continue;
        }
        result[it.key()] = it->editor->value(&field_error);
        if (!field_error.isEmpty()) {
            if (error) {
                *error = wosdbg::display_label(it.key()) + ": " + field_error;
            }
            return {};
        }
    }
    if (error) {
        error->clear();
    }
    return result;
}
