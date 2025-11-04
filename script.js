const sectionsData = [
  {
    id: 'start-times-hours',
    title: 'Start Times & Hours',
    body: [
      {
        type: 'paragraph',
        text: 'Expect occasional longer shifts when games go to overtime, extra innings, or experience delays. Finish the trading session if you can stay.'
      },
      {
        type: 'list',
        items: [
          'Take back the extra time at your next opportunity by starting a later shift or stepping away on an IN shift.',
          'Update your shift time in Teams and let colleagues know when you adjust coverage.',
          'If you have a hard stop, communicate early and hand over responsibly.'
        ]
      },
      {
        type: 'paragraph',
        text: 'Full schedule specifics live in the scheduling guide, but the expectation is that flexibility is a shared team effort.'
      }
    ]
  },
  {
    id: 'start-ready',
    title: 'Being Ready at Start Time',
    body: [
      {
        type: 'paragraph',
        text: 'If you are scheduled at 8:00 a.m., be logged in and ready to trade at 8:00 a.m., not still setting up.'
      },
      {
        type: 'list',
        items: [
          'Give yourself 10-15 minutes before the shift if you need to catch up from the prior day.',
          'If that prep time stacks up into an hour of extra effort over a week, feel free to reclaim it later.',
          'After five or more days away, we try to schedule an IN shift to help you reset, although it is not always possible.'
        ]
      }
    ]
  },
  {
    id: 'role-expectations',
    title: 'Role-Based Expectations',
    body: [
      {
        type: 'paragraph',
        text: 'Leaders, sport owners, and trading managers often keep longer hours or take global calls outside their scheduled shifts.'
      },
      {
        type: 'list',
        items: [
          'Their wider communication responsibilities mean earlier starts or later evenings.',
          'These expectations come with higher-level compensation and fewer weekend shifts to balance the load.',
          'New starters are not expected to mirror these extended hours.'
        ]
      }
    ]
  },
  {
    id: 'off-hours',
    title: 'Off-Hours Boundaries',
    body: [
      {
        type: 'paragraph',
        text: 'You are not expected to check Slack or email outside rostered hours as a new starter.'
      },
      {
        type: 'list',
        items: [
          'No requirement to install Slack or Outlook on your phone or to answer calls on days off.',
          'If your role evolves, expectations may shift alongside compensation changes.',
          'Enjoy your time off guilt-free; staying plugged in is optional.'
        ]
      }
    ]
  },
  {
    id: 'lunch-breaks',
    title: 'Lunch Breaks',
    body: [
      {
        type: 'paragraph',
        text: 'Weekday lunch breaks can be away from the desk when you are not live trading.'
      },
      {
        type: 'list',
        items: [
          'Aim for around 30 minutes, but a little longer is fine as long as coverage is clear.',
          'Stagger lunch times so everyone working the same sport is not away at once.',
          'Use the kitchen, head outside, or hit the restaurant, whatever works best.',
          'On weekends, eat at your desk; we cover weekend lunch for that reason.',
          'Always let teammates know when you are stepping away and when you will be back.'
        ]
      }
    ]
  },
  {
    id: 'signing-on',
    title: 'Signing On & Off',
    body: [
      {
        type: 'paragraph',
        text: "Kick off the day with a hello in the #natt Slack channel's Morning Thread and make yourself active."
      },
      {
        type: 'list',
        items: [
          'Reply to the thread as you log on so teammates know you are available.',
          'Mute the thread if the notifications get noisy by using the three-dot menu.',
          'End of day, add a quick sign-off message so the team knows you are done.',
          'Link your Slack and Outlook so out-of-office calendar events automatically update your Slack status.'
        ]
      }
    ]
  },
  {
    id: 'catching-up',
    title: 'Catching Up Before a Shift',
    body: [
      {
        type: 'paragraph',
        text: 'Before every operational shift, align on the latest information.'
      },
      {
        type: 'list',
        items: [
          'Read the handover for your sport from Melbourne overnight.',
          "Review messages in your sport's trading channel and #fanduel-trading.",
          'Listen in on the 11:00 a.m. ET admin shift summary.',
          'For live trading, skim the live trading callouts channel for real-time updates.',
          'Optional but helpful: read overnight risk handovers, admin notes, and wider channel summaries.'
        ]
      }
    ]
  },
  {
    id: 'staying-informed',
    title: 'Staying Informed',
    body: [
      {
        type: 'paragraph',
        text: 'Company-wide sessions keep everyone up to date on business moves.'
      },
      {
        type: 'list',
        items: [
          'Attend Sports All Hands or company Town Halls when you are working.',
          'Trading leaders share notes so you can catch up if you miss a session.',
          'Review those notes or recordings when you have time, even if you were off that day.'
        ]
      }
    ]
  },
  {
    id: 'office-etiquette',
    title: 'Office Etiquette',
    body: [
      {
        type: 'paragraph',
        text: 'Respect our hot-desking setup and keep the shared office comfortable for everyone.'
      },
      {
        type: 'list',
        items: [
          'Tidy your space and clean up after meals; use recycling bins properly.',
          'Any PC can be accessed with your Okta credentials; docking stations are plug-and-play.',
          'Snacks and drinks are for everyone; please avoid stockpiling, especially alcohol.',
          'Dispose of weekend catering packaging promptly.',
          'Keep desks clear so the next person has a fresh space.',
          'First aid supplies are in the kitchen if needed.'
        ]
      }
    ]
  },
  {
    id: 'sick-days',
    title: 'Sick Days',
    body: [
      {
        type: 'paragraph',
        text: 'You begin each calendar year with 80 hours (10 days) of sick leave.'
      },
      {
        type: 'list',
        items: [
          'Message your manager, or another leader if they are away, as soon as you know you are out so coverage can be arranged.',
          'Log sick days in Dayforce as weekdays, even if the absence was on a weekend.',
          "If you use all 10 days, provide a doctor's certificate; no one has been denied sick leave so far.",
          'For absences longer than five consecutive days, the benefits team must be notified (no extra action required from you).',
          'Check the Trading HR page for additional wellbeing support.'
        ]
      }
    ]
  },
  {
    id: 'whatsapp',
    title: 'Trading WhatsApp Group',
    body: [
      {
        type: 'paragraph',
        text: 'A dedicated WhatsApp group supports urgent coverage needs.'
      },
      {
        type: 'list',
        items: [
          'It is used for emergencies such as last-minute trading coverage.',
          'Download WhatsApp and ask to be added so you can receive critical alerts.'
        ]
      }
    ]
  },
  {
    id: 'module-completion',
    title: 'Training Module Completion',
    body: [
      {
        type: 'paragraph',
        text: 'Completing assigned trading modules on time is part of your annual goals.'
      },
      {
        type: 'list',
        items: [
          'Find modules in Okta under Training Camp and finish them by the stated due date.',
          'Overdue modules can suspend your access to trading tools and create regulatory risk.',
          'You will receive email reminders; leaders also post due dates in calls and on the #natt Canvas.',
          'Check for upcoming deadlines before going on vacation and complete anything due while you are away.'
        ]
      }
    ]
  },
  {
    id: 'dge-license',
    title: 'DGE License',
    body: [
      {
        type: 'paragraph',
        text: 'All traders must obtain their New Jersey DGE license.'
      },
      {
        type: 'list',
        items: [
          'Email fdglicensing@fanduel.com to start the process and ask your manager for next steps.',
          'Expect to visit the DGE office in Atlantic City; travel, lodging, and meals are covered.',
          'Car pool with teammates if possible; time spent counts as a paid workday.'
        ]
      }
    ]
  },
  {
    id: 'employee-play',
    title: 'Employee Play Policy',
    body: [
      {
        type: 'paragraph',
        text: "Adhere to FanDuel's employee play policy to avoid regulatory issues."
      },
      {
        type: 'list',
        items: [
          'Review the Employee Play Map and full policy to see where and with whom you can bet.',
          'New Jersey traders may place bets, but DraftKings usage is prohibited under the DGE license.',
          'Track your personal betting activity to stay responsible and aware of results.',
          'Use the #employee-play-inquiries Slack channel when you have questions.'
        ]
      }
    ]
  }
];

const searchForm = document.querySelector('#search-form');
const searchInput = document.querySelector('#search-input');
const heroNote = document.querySelector('.hero-note');
const defaultHeroNote = heroNote?.textContent.trim() ?? '';
const sectionsContainer = document.querySelector('#sections');
const navList = document.querySelector('#section-nav');
const template = document.querySelector('#section-template');

let observer;
let activeSectionId = sectionsData[0]?.id ?? '';

const escapeRegExp = (value) => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

const applyHighlight = (text, query) => {
  if (!query) {
    return text;
  }
  const safe = escapeRegExp(query.trim());
  if (!safe) {
    return text;
  }
  const regex = new RegExp(`(${safe})`, 'gi');
  return text.replace(regex, '<mark class="highlight">$1</mark>');
};

sectionsData.forEach((section) => {
  const searchableFragments = section.body.flatMap((block) => {
    if (block.type === 'paragraph') {
      return block.text;
    }
    if (block.type === 'list') {
      return block.items;
    }
    if (block.type === 'heading') {
      return block.text;
    }
    return '';
  });
  section.searchText = [section.title, ...searchableFragments].join(' ').toLowerCase();
});

const renderNav = () => {
  navList.innerHTML = '';
  const fragment = document.createDocumentFragment();
  sectionsData.forEach((section, index) => {
    const item = document.createElement('li');
    const button = document.createElement('button');
    button.type = 'button';
    button.dataset.sectionId = section.id;
    button.textContent = section.title;
    if (index === 0) {
      button.classList.add('is-active');
    }
    button.addEventListener('click', () => {
      const target = document.getElementById(section.id);
      if (target) {
        target.scrollIntoView({ behavior: 'smooth', block: 'start' });
        activeSectionId = section.id;
        updateNavActive();
      }
    });
    item.appendChild(button);
    fragment.appendChild(item);
  });
  navList.appendChild(fragment);
};

const renderSections = ({ query = '' } = {}) => {
  const normalized = query.trim().toLowerCase();
  sectionsContainer.innerHTML = '';
  const fragment = document.createDocumentFragment();
  let matches = 0;

  sectionsData.forEach((section) => {
    const isMatch = normalized ? section.searchText.includes(normalized) : false;
    if (isMatch) {
      matches += 1;
    }

    const clone = template.content.cloneNode(true);
    const card = clone.querySelector('.section-card');
    const title = clone.querySelector('.section-title');
    const body = clone.querySelector('.section-body');

    card.id = section.id;
    if (isMatch) {
      card.classList.add('is-match');
    }

    title.textContent = section.title;

    section.body.forEach((block) => {
      if (block.type === 'paragraph') {
        const p = document.createElement('p');
        p.innerHTML = applyHighlight(block.text, query);
        body.appendChild(p);
      }

      if (block.type === 'heading') {
        const h4 = document.createElement('h4');
        h4.textContent = block.text;
        body.appendChild(h4);
      }

      if (block.type === 'list') {
        const ul = document.createElement('ul');
        block.items.forEach((item) => {
          const li = document.createElement('li');
          li.innerHTML = applyHighlight(item, query);
          ul.appendChild(li);
        });
        body.appendChild(ul);
      }
    });

    fragment.appendChild(clone);
  });

  sectionsContainer.appendChild(fragment);
  setupObserver();
  return matches;
};

const updateNavActive = () => {
  const buttons = navList.querySelectorAll('button');
  buttons.forEach((button) => {
    button.classList.toggle('is-active', button.dataset.sectionId === activeSectionId);
  });
};

const setupObserver = () => {
  if (observer) {
    observer.disconnect();
  }

  observer = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (entry.isIntersecting) {
          activeSectionId = entry.target.id;
        }
      });
      updateNavActive();
    },
    {
      root: null,
      threshold: 0,
      rootMargin: '-50% 0px -50% 0px'
    }
  );

  document.querySelectorAll('.section-card').forEach((card) => observer.observe(card));
};

const scrollToFirstMatch = () => {
  const match = document.querySelector('.section-card.is-match');
  if (match) {
    match.scrollIntoView({ behavior: 'smooth', block: 'start' });
    return true;
  }
  const first = document.querySelector('.section-card');
  if (first) {
    first.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }
  return false;
};

const handleSearch = (event) => {
  event.preventDefault();
  const rawQuery = searchInput.value.trim();

  if (!rawQuery) {
    renderSections();
    heroNote.textContent = defaultHeroNote;
    scrollToFirstMatch();
    return;
  }

  const matches = renderSections({ query: rawQuery });
  if (matches === 0) {
    heroNote.textContent = `No matches found for “${rawQuery}”. Showing all sections instead.`;
    renderSections();
    scrollToFirstMatch();
    return;
  }

  const plural = matches === 1 ? 'section' : 'sections';
  heroNote.textContent = `Showing ${matches} ${plural} for “${rawQuery}”.`;
  scrollToFirstMatch();
};

const handleSearchInput = () => {
  if (searchInput.value === '') {
    renderSections();
    heroNote.textContent = defaultHeroNote;
  }
};

renderNav();
renderSections();

searchForm?.addEventListener('submit', handleSearch);
searchInput?.addEventListener('input', handleSearchInput);
searchInput?.addEventListener('search', handleSearchInput);
