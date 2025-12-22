const { WebClient } = require('@slack/web-api');

const slack = new WebClient(process.env.SLACK_BOT_TOKEN);

module.exports = async (req, res) => {
  try {
    const body = req.body;

    // Slack URL verification (challenge)
    if (body.type === 'url_verification') {
      return res.status(200).json({ challenge: body.challenge });
    }

    // Event callback
    if (body.type === 'event_callback') {
      const event = body.event;
      
      // 여기에 이벤트 처리 로직 추가
      console.log('Event received:', event.type);
      
      // 중복 처리 방지를 위해 빠르게 응답
      res.status(200).send('OK');
      return;
    }

    res.status(200).send('OK');
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: error.message });
  }
};
