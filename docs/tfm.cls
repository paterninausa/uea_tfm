\ProvidesClass{tfm}[2024/03/14 Custom Class for TFM]
\LoadClass[11pt,a4paper]{report}

% Paquetes requeridos
\RequirePackage[utf8]{inputenc}
\RequirePackage[T1]{fontenc}
\RequirePackage[english,spanish,es-lcroman, es-tabla]{babel}
\RequirePackage{graphicx}
\RequirePackage{geometry}
\RequirePackage{fancyhdr}
\RequirePackage[scaled]{helvet} % Usa la fuente Helvetica, que es similar a Arial
\RequirePackage{setspace}
\RequirePackage[style=ieee]{biblatex}
\RequirePackage{xcolor}
\definecolor{rojo}{RGB}{255,0,0}

\bibliography{bibliography}
\usepackage{csquotes}

\usepackage{caption}
\captionsetup{font=footnotesize}
\captionsetup{justification=centering}
\captionsetup[table]{position=bottom}

\usepackage{parskip}
\setlength{\parindent}{0pt}

\usepackage{titlesec}
\titleformat{\chapter}[block] % 'block' para que esté en la misma línea
  {\normalfont\fontsize{20pt}{20pt}\selectfont\bfseries} % Estilos de fuente para el título
  {Capítulo \thechapter.   } % Prefijo del título
  {0pt} % Espacio entre el prefijo y el título del capítulo
  {} % Antes del título
\titlespacing*{\chapter}{0pt}{-10pt}{0pt}

\titleformat{\section}
  {\normalfont\fontsize{14pt}{14pt}\selectfont\bfseries}
  {\thesection}
  {1em}
  {}
\titlespacing*{\section}{0pt}{0pt}{0pt} % Espacio antes y después del título de la sección

\usepackage{enumitem}
\setlist[itemize]{itemsep=0pt, parsep=0pt, label=\textbullet}

\addto\captionsspanish{ \renewcommand{\contentsname}{Índice general}}

% Configura la fuente predeterminada a sans-serif (Helvet/Arial)
\renewcommand{\familydefault}{\sfdefault}

% Configuraciones de geometría de la página
\geometry{left=3cm, right=3cm, top=1.3cm, bottom=1.8cm, includeheadfoot, headheight=40pt}

% Estilo de página para la portada (sin encabezados o pies de página)
\fancypagestyle{titlepagestyle}{
    \fancyhf{} % Limpia el encabezado y pie de página
    \renewcommand{\headrulewidth}{0pt} % Elimina la línea del encabezado
    \renewcommand{\footrulewidth}{0pt} % Elimina la línea del pie de página
}

% Configuración de Fancyhdr para páginas regulares
\pagestyle{fancy} % Configura el estilo de página a "fancy"
    \fancyhf{} % Limpia el encabezado y pie de página
    \lhead{\begin{tabular}{p{9cm}}\vspace{0.05em}\fontsize{10.5pt}{13pt}\selectfont \Titulo{}\\[0.5em]\fontsize{10.5pt}{13pt}\selectfont \NombreAutor{}\vspace{0.45em}\end{tabular}} % Encabezado izquierdo con tu nombre
    \rhead{\raisebox{-0.4\height}{\includegraphics[height=1cm]{imagenes/logo_ue.png}}} % Encabezado derecho con el nombre de la universidad
    \cfoot{\thepage} % Pie de página con el número de página
    \renewcommand{\headrulewidth}{0.7pt} % Establece el grosor de la línea del encabezado
    \setlength{\headheight}{57.30338pt} % Increase headheight
    
% Redefinir el estilo de página 'plain' para que coincida con 'fancy'
\fancypagestyle{plain}{
    \fancyhf{} % Limpia el encabezado y pie de página
    \lhead{\begin{tabular}{p{9cm}}\vspace{0.05em}\fontsize{10.5pt}{13pt}\selectfont \Titulo{}\\[0.5em]\fontsize{10.5pt}{13pt}\selectfont \NombreAutor{}\vspace{0.45em}\end{tabular}} % Encabezado izquierdo con tu nombre
    \rhead{\raisebox{-0.4\height}{\includegraphics[height=1cm]{imagenes/logo_ue.png}}} % Encabezado derecho con el nombre de la universidad
    \cfoot{\thepage} % Pie de página con el número de página
    \renewcommand{\headrulewidth}{0.7pt} % Establece el grosor de la línea del encabezado
    \setlength{\headheight}{57.30338pt} % Increase headheight
}

% Espaciado interlineal
\onehalfspacing
\setstretch{1.1}

% Configuración de la numeración y los títulos de figuras y tablas
\captionsetup[figure]{labelfont=bf, name=Figura, labelsep=period}
\captionsetup[table]{labelfont=bf, name=Tabla, labelsep=period}
