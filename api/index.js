module.exports = (req, res) => {
  res.status(200).json({
    status: '✅ Running',
    name: 'CEO Daily Report Bot',
    description: 'Slack/Notion/매출 데이터를 분석하여 CEO에게 일일 리포트 발송',
    endpoints: {
      daily: 'GET /api/cron - 일일 분석 실행',
      initial: 'GET /api/cron?days=7 - 최초 7일 종합 분석',
      events: 'POST /api/events - Slack 이벤트 수신',
    },
    timestamp: new Date().toISOString(),
  });
};
