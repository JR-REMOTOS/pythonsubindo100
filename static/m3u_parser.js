export function parseM3U(m3uContent) {
    const lines = m3uContent.split(/[\r\n]+/).map(line => line.trim()).filter(line => line);
    const items = [];
    let currentItem = null;

    for (let i = 0; i < lines.length; i++) {
        const line = lines[i];

        if (line.startsWith('#EXTINF:')) {
            currentItem = parseExtinf(line);
            if (i + 1 < lines.length && lines[i + 1].startsWith('http')) {
                currentItem.url = lines[i + 1];
                items.push(currentItem);
                i++; // Skip the URL line
            }
        }
    }
    return items;
}

function parseExtinf(line) {
    const info = {};
    const parts = line.split(',');
    info.name = parts.length > 1 ? parts[1].trim() : '';

    const tvgIdMatch = line.match(/tvg-id="([^"]*)"/);
    info.tvg_id = tvgIdMatch ? tvgIdMatch[1] : '';

    const tvgNameMatch = line.match(/tvg-name="([^"]*)"/);
    info.tvg_name = tvgNameMatch ? tvgNameMatch[1] : '';

    const tvgLogoMatch = line.match(/tvg-logo="([^"]*)"/);
    info.tvg_logo = tvgLogoMatch ? tvgLogoMatch[1] : '';

    const groupTitleMatch = line.match(/group-title="([^"]*)"/);
    info.group_title = groupTitleMatch ? groupTitleMatch[1] : '';
    
    info.channel_name = info.name;

    return info;
}

export function categorizeItems(items) {
    const movies = [];
    const series = [];
    const channels = [];

    items.forEach(item => {
        const type = _categorize_entry(item.group_title, item.channel_name);
        item.type = type;

        if (type === 'filme') {
            movies.push(item);
        } else if (type === 'serie') {
            series.push(item);
        } else {
            channels.push(item);
        }
    });

    return { movies, series, channels };
}

function _categorize_entry(group_title, channel_name) {
    const lower_group = group_title.toLowerCase();
    const lower_name = channel_name.toLowerCase();
    let type = 'filme'; // Default to movie

    const live_keywords = [
        'live', 'tv ', 'canal', 'channel', 'ao vivo', 'iptv',
        'radio', 'rádio', 'news', 'notícias', 'sport', 'esporte',
        'entertainment', 'music', 'kids', 'documentary', 'religioso',
        'brasil', 'globo', 'sbt', 'record', 'band', 'fox ', 'cnn',
        'discovery', 'cartoon', 'disney', 'hbo ', 'premium'
    ];

    const series_keywords = [
        'series', 'série', 'temporada', 'season', 'episodio', 'episode',
        's01', 's02', 's03', 's04', 's05', 's06', 's07', 's08', 's09',
        'e01', 'e02', 'e03', 'e04', 'e05', 'e06', 'e07', 'e08', 'e09',
        'x01', 'x02', 'x03', 'x04', 'x05', 'cap ', 'capitulo'
    ];

    if (live_keywords.some(keyword => lower_group.includes(keyword)) || live_keywords.some(keyword => lower_name.includes(keyword))) {
        type = 'canal';
    }
    
    if (series_keywords.some(keyword => lower_group.includes(keyword)) || series_keywords.some(keyword => lower_name.includes(keyword)) || lower_name.match(/\bs\d{1,2}e\d{1,2}\b|\bseason\s+\d+.*episode\s+\d+|\b\d{1,2}x\d{1,2}\b|\btemporada\s+\d+.*episodio\s+\d+/)) {
        type = 'serie';
    }
    
    if (['filmes', 'movies', 'vod', 'crime', 'lancamentos'].some(keyword => lower_group.includes(keyword))) {
        type = 'filme';
    }

    console.log(`[Categorization] Group: "${group_title}", Name: "${channel_name}", Type: ${type}`);
    return type;
}
