#pragma once

#include <QJsonObject>
#include <QMap>
#include <QWidget>

class QCheckBox;
class QFormLayout;
class ToolArgumentField;

// Optional fields are omitted until enabled, preserving backend defaults.
class ToolArgumentsForm final : public QWidget {
    Q_OBJECT
   public:
    explicit ToolArgumentsForm(QWidget* parent = nullptr);
    void set_schema(const QJsonObject& schema, const QJsonObject& suggestions = {});
    [[nodiscard]] QJsonObject arguments(QString* error = nullptr) const;

   signals:
    void changed();

   private:
    struct Field {
        ToolArgumentField* editor;
        QCheckBox* enabled;
    };
    QFormLayout* form;
    QMap<QString, Field> fields;
};
