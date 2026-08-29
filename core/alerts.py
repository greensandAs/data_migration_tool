"""core/alerts.py — Alert rule evaluation and webhook dispatch.

Evaluates active rules from DMT_ALERT_RULES against recent pipeline runs,
logs triggered alerts to DMT_ALERT_LOG, and sends webhook notifications
to Slack, Teams, Google Chat, or custom endpoints.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from urllib.request import Request, urlopen
from urllib.error import URLError

log = logging.getLogger(__name__)

_RULES_TABLE = "HISTLOAD_DB.META.DMT_ALERT_RULES"
_LOG_TABLE = "HISTLOAD_DB.META.DMT_ALERT_LOG"


def evaluate_and_fire(conn, run_context: dict | None = None):
    """Evaluate all active alert rules and fire matching ones.

    run_context: optional dict with keys from the just-completed run:
        {source_table, status, failed_step, error_message, duration_sec,
         rows_extracted, rows_loaded, batch_id, connection_profile}
    """
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT * FROM {_RULES_TABLE} WHERE ACTIVE = TRUE")
        cols = [d[0] for d in cur.description]
        rules = [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as e:
        log.warning("Could not read alert rules: %s", e)
        return
    finally:
        cur.close()

    for rule in rules:
        try:
            triggered, message = _check_rule(conn, rule, run_context)
            if triggered:
                _fire_alert(conn, rule, message)
        except Exception as e:
            log.warning("Error evaluating rule %s: %s", rule.get("RULE_NAME"), e)


def _check_rule(conn, rule: dict, ctx: dict | None) -> tuple[bool, str]:
    """Return (triggered: bool, message: str) for a single rule."""
    ctype = rule.get("CONDITION_TYPE", "")
    threshold = int(rule.get("THRESHOLD") or 0)
    scope = rule.get("TABLE_SCOPE")  # optional table name filter

    if ctype == "RUN_FAILED":
        if ctx and ctx.get("status") == "failed":
            tbl = ctx.get("source_table", "?")
            if scope and scope.upper() != tbl.upper():
                return False, ""
            step = ctx.get("failed_step", "?")
            err = (ctx.get("error_message") or "")[:200]
            return True, f"Pipeline failed for {tbl} at step '{step}': {err}"

    elif ctype == "TABLE_STALE":
        cur = conn.cursor()
        try:
            scope_clause = f"AND UPPER(SOURCE_TABLE) = UPPER('{scope}')" if scope else ""
            cur.execute(f"""
                SELECT SOURCE_TABLE, LAST_LOADED_AT
                FROM HISTLOAD_DB.META.MIGRATION_CONFIG
                WHERE ACTIVE = TRUE {scope_clause}
                  AND (LAST_LOADED_AT IS NULL
                       OR LAST_LOADED_AT < DATEADD('hour', -{threshold}, CURRENT_TIMESTAMP()))
            """)
            stale = cur.fetchall()
            if stale:
                names = ", ".join(r[0] for r in stale[:5])
                more = f" (+{len(stale)-5} more)" if len(stale) > 5 else ""
                return True, f"{len(stale)} table(s) stale >{threshold}h: {names}{more}"
        finally:
            cur.close()

    elif ctype == "ROW_DRIFT_PCT":
        if ctx and ctx.get("status") == "success":
            extracted = ctx.get("rows_extracted") or 0
            loaded = ctx.get("rows_loaded") or 0
            if extracted > 0:
                drift_pct = abs(extracted - loaded) / extracted * 100
                if drift_pct > threshold:
                    tbl = ctx.get("source_table", "?")
                    if scope and scope.upper() != tbl.upper():
                        return False, ""
                    return True, (f"Row drift {drift_pct:.1f}% for {tbl} "
                                  f"(extracted={extracted}, loaded={loaded})")

    elif ctype == "CONSECUTIVE_FAILURES":
        cur = conn.cursor()
        try:
            scope_clause = f"AND UPPER(SOURCE_TABLE) = UPPER('{scope}')" if scope else ""
            cur.execute(f"""
                SELECT SOURCE_TABLE, COUNT(*) AS FAILS
                FROM (
                    SELECT SOURCE_TABLE, STATUS,
                           ROW_NUMBER() OVER (PARTITION BY SOURCE_TABLE
                                              ORDER BY INSERTED_AT DESC) AS RN
                    FROM HISTLOAD_DB.META.RUN_LOG
                    WHERE 1=1 {scope_clause}
                )
                WHERE RN <= {threshold} AND STATUS = 'failed'
                GROUP BY SOURCE_TABLE
                HAVING COUNT(*) >= {threshold}
            """)
            consec = cur.fetchall()
            if consec:
                names = ", ".join(r[0] for r in consec[:5])
                return True, f"{len(consec)} table(s) with {threshold}+ consecutive failures: {names}"
        finally:
            cur.close()

    return False, ""


def _fire_alert(conn, rule: dict, message: str):
    """Log alert and dispatch webhook."""
    cur = conn.cursor()
    try:
        cur.execute(f"""
            INSERT INTO {_LOG_TABLE}
            (ALERT_ID, RULE_ID, RULE_NAME, CONDITION_TYPE, TABLE_NAME,
             MESSAGE, ACTION_TAKEN, TRIGGERED_AT)
            VALUES (UUID_STRING(), %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
        """, (rule.get("RULE_ID"), rule.get("RULE_NAME"),
              rule.get("CONDITION_TYPE"), rule.get("TABLE_SCOPE"),
              message, rule.get("ACTION_TYPE")))
        conn.commit()
    except Exception as e:
        log.warning("Failed to log alert: %s", e)
    finally:
        cur.close()

    action = rule.get("ACTION_TYPE", "LOG_ONLY")
    url = rule.get("WEBHOOK_URL")
    if action == "LOG_ONLY" or not url:
        return

    try:
        payload = _build_payload(action, rule, message)
        _send_webhook(url, payload)
        log.info("Alert sent via %s for rule '%s'", action, rule.get("RULE_NAME"))
    except Exception as e:
        log.warning("Webhook dispatch failed for rule '%s': %s", rule.get("RULE_NAME"), e)


def _build_payload(action: str, rule: dict, message: str) -> dict:
    """Build platform-specific JSON payload."""
    title = f"MigrateX Alert: {rule.get('RULE_NAME', 'Alert')}"
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    if action == "WEBHOOK_SLACK":
        return {
            "text": f"*{title}*\n{message}\n_{ts}_",
            "blocks": [
                {"type": "header", "text": {"type": "plain_text", "text": title}},
                {"type": "section", "text": {"type": "mrkdwn",
                    "text": f"*Condition:* `{rule.get('CONDITION_TYPE')}`\n"
                            f"*Message:* {message}\n*Time:* {ts}"}},
            ]
        }

    elif action == "WEBHOOK_TEAMS":
        return {
            "@type": "MessageCard",
            "themeColor": "F85149",
            "summary": title,
            "sections": [{
                "activityTitle": title,
                "facts": [
                    {"name": "Condition", "value": rule.get("CONDITION_TYPE", "?")},
                    {"name": "Message", "value": message},
                    {"name": "Time", "value": ts},
                ],
                "markdown": True,
            }]
        }

    elif action == "WEBHOOK_GCHAT":
        return {
            "cardsV2": [{
                "cardId": "migratex-alert",
                "card": {
                    "header": {
                        "title": title,
                        "subtitle": ts,
                        "imageUrl": "",
                        "imageType": "CIRCLE",
                    },
                    "sections": [{
                        "header": "Alert Details",
                        "widgets": [
                            {"decoratedText": {
                                "topLabel": "Condition",
                                "text": rule.get("CONDITION_TYPE", "?")}},
                            {"decoratedText": {
                                "topLabel": "Details",
                                "text": message}},
                            {"decoratedText": {
                                "topLabel": "Table Scope",
                                "text": rule.get("TABLE_SCOPE") or "All tables"}},
                        ]
                    }]
                }
            }]
        }

    # WEBHOOK_CUSTOM — generic JSON
    return {
        "title": title,
        "condition": rule.get("CONDITION_TYPE"),
        "message": message,
        "timestamp": ts,
        "rule_name": rule.get("RULE_NAME"),
        "table_scope": rule.get("TABLE_SCOPE"),
    }


def _send_webhook(url: str, payload: dict):
    """Send JSON POST to webhook URL."""
    data = json.dumps(payload).encode("utf-8")
    req = Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urlopen(req, timeout=10) as resp:
            log.debug("Webhook response: %s", resp.status)
    except URLError as e:
        log.warning("Webhook failed: %s", e)
        raise


def send_test_alert(url: str, action_type: str) -> str:
    """Send a test alert to verify webhook connectivity. Returns status message."""
    test_rule = {
        "RULE_NAME": "Test Alert",
        "CONDITION_TYPE": "TEST",
        "TABLE_SCOPE": None,
    }
    payload = _build_payload(action_type, test_rule, "This is a test alert from MigrateX.")
    try:
        _send_webhook(url, payload)
        return "Test alert sent successfully!"
    except Exception as e:
        return f"Failed to send test alert: {e}"
