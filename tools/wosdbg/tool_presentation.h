#pragma once

#include <QJsonObject>
#include <QString>

namespace wosdbg {
// Presentation metadata augments the canonical catalog; it never dispatches tools.
QString display_label(QString name);
QJsonObject describe_tool(QJsonObject tool);
QString result_summary(const QJsonObject& result);
}  // namespace wosdbg
